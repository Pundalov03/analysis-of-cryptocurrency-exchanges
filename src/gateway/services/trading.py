# trading.py - ИСПРАВЛЕННАЯ ВЕРСИЯ
import logging
import sys
import os
import time
import asyncio
import aiohttp
from typing import Dict, Optional
from django.utils import timezone

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

try:
    import django

    django.setup()
    from app.models import Trade, Exchange
    from django.conf import settings
except ImportError as e:
    logging.error(f"Failed to setup Django: {e}")
    raise

logger = logging.getLogger(__name__)


class TradingService:
    def __init__(self, binance_ws_instance):
        self.ws = binance_ws_instance
        self.user_id = binance_ws_instance.user_id

        # Загружаем конфигурацию
        trading_config = getattr(settings, 'TRADING_CONFIG', {})

        # Проверяем, что target_profit_percent не 0
        target_profit_default = max(trading_config.get('DEFAULT_TARGET_PROFIT', 1.0), 0.5)

        self.config = {
            'target_profit_percent': getattr(settings, 'TARGET_PROFIT_PERCENT', target_profit_default),
            'stop_loss_percent': getattr(settings, 'STOP_LOSS_PERCENT',
                                         trading_config.get('DEFAULT_STOP_LOSS', 0.5)),
            'min_trade_amount_usd': trading_config.get('MIN_TRADE_AMOUNT_USD', 10.0),
            'commission_rate': trading_config.get('COMMISSION_RATE', 0.001),
            'auto_close_enabled': trading_config.get('AUTO_CLOSE_ENABLED', True),
            'max_retries': 3,
            'retry_delay': 1.0,
            'cache_ttl': 60,
        }

        # Проверяем минимальные значения
        if self.config['target_profit_percent'] < 0.5:
            logger.warning(f"⚠️ Target profit too low ({self.config['target_profit_percent']}%), setting to 0.5%")
            self.config['target_profit_percent'] = 0.5

        logger.info(f"📊 TradingService initialized:")
        logger.info(f"   Target profit: {self.config['target_profit_percent']}%")
        logger.info(f"   Stop loss: {self.config['stop_loss_percent']}%")
        logger.info(f"   Commission: {self.config['commission_rate'] * 100}%")
        logger.info(f"   Min trade: ${self.config['min_trade_amount_usd']}")

        # Кеш
        self._cache = {
            'lot_size_info': {},
            'symbol_info': {}
        }
        self._cache_timestamps = {}

    async def buy_with_usdt(self, symbol: str, usdt_amount: float) -> Optional[Dict]:
        """Покупка монеты на определенную сумму в USDT"""
        try:
            # Получаем текущую цену
            current_price = await self._get_current_price(symbol)

            if current_price <= 0:
                logger.error(f"❌ Invalid current price for {symbol}: ${current_price}")
                return None

            # Рассчитываем количество
            quantity = usdt_amount / current_price

            # Форматируем количество
            lot_size_info = await self._get_lot_size_info(symbol)
            formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)
            quantity_float = float(formatted_quantity)

            # Проверяем баланс
            if not await self._check_usdt_balance(usdt_amount):
                logger.error(f"❌ Insufficient USDT balance. Required: ${usdt_amount:.2f}")
                return None

            # Создаем ордер на покупку
            order_result = await self._create_market_buy_order(symbol, quantity_float)

            if order_result:
                # После успешной покупки подписываемся на обновления цены
                await self._subscribe_to_symbol_after_purchase(symbol)

                # Сохраняем сделку в БД
                await self._save_trade_to_db_and_list(symbol, order_result, current_price, quantity_float)

                return order_result

        except Exception as e:
            logger.error(f"❌ Error buying with USDT: {e}")
            return None

    async def buy_with_usdt_optimized(self, symbol: str, usdt_amount: float,
                                      create_trade_record: bool = True) -> Optional[Dict]:
        """Покупка с контролем создания сделки"""
        try:
            # Получаем текущую цену
            current_price = await self._get_current_price(symbol)

            if current_price <= 0:
                logger.error(f"❌ Invalid current price for {symbol}: ${current_price}")
                return None

            # Рассчитываем количество
            quantity = usdt_amount / current_price

            # Форматируем количество
            lot_size_info = await self._get_lot_size_info(symbol)
            formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)
            quantity_float = float(formatted_quantity)

            # Проверяем баланс
            if not await self._check_usdt_balance(usdt_amount):
                logger.error(f"❌ Insufficient USDT balance. Required: ${usdt_amount:.2f}")
                return None

            # Создаем ордер на покупку
            order_result = await self._create_market_buy_order(symbol, quantity_float)

            if order_result:
                # Сохраняем ID ордера для предотвращения дублирования
                order_id = order_result.get('orderId')
                logger.info(f"📝 Order created with ID: {order_id}")

                # После успешной покупки подписываемся на обновления цены
                await self._subscribe_to_symbol_after_purchase(symbol)

                # Сохраняем сделку в БД только если нужно
                if create_trade_record:
                    await self._save_trade_to_db_and_list(symbol, order_result, current_price, quantity_float)
                else:
                    logger.info("ℹ️ Trade record creation skipped (will be created by WebSocket)")

                return order_result

        except Exception as e:
            logger.error(f"❌ Error buying with USDT: {e}")
            return None

    async def _subscribe_to_symbol_after_purchase(self, symbol: str):
        """Подписка на обновления цены для нового символа"""
        try:
            symbol_lower = symbol.lower()

            if hasattr(self.ws, 'active_streams'):
                stream_name = f"{symbol_lower}@trade"

                if stream_name not in self.ws.active_streams:
                    logger.info(f"📡 Subscribing to {symbol_lower} after purchase...")

                    await self.ws.subscribe_to_market_data_stream(stream_name)

                    logger.info(f"✅ Successfully subscribed to {symbol_lower}")
                else:
                    logger.debug(f"ℹ️ Already subscribed to {symbol_lower}")
            else:
                logger.warning(f"⚠️ Cannot subscribe: active_streams not found")

        except Exception as e:
            logger.error(f"❌ Error subscribing after purchase: {e}")

    async def _create_market_buy_order(self, symbol: str, quantity: float) -> Optional[Dict]:
        """Создание рыночного ордера на покупку"""
        for attempt in range(self.config['max_retries']):
            try:
                # Синхронизируем время
                await self.ws.sync_time()

                # Получаем информацию о лот-сайзе
                lot_size_info = await self._get_lot_size_info(symbol)
                formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)

                # Получаем текущую цену для проверки суммы
                current_price = await self._get_current_price(symbol)
                trade_amount_usd = float(formatted_quantity) * current_price

                # Проверяем минимальную сумму сделки
                if trade_amount_usd < self.config['min_trade_amount_usd']:
                    logger.warning(f"⚠️ Trade amount ${trade_amount_usd:.2f} below minimum "
                                   f"${self.config['min_trade_amount_usd']}, skipping")
                    return None

                # Правильные параметры для MARKET BUY ордера
                params = {
                    'symbol': symbol.upper(),
                    'side': 'BUY',
                    'type': 'MARKET',
                    'quantity': formatted_quantity,
                    'timestamp': self.ws._get_timestamp(),
                    'recvWindow': 5000,
                    'newOrderRespType': 'FULL'  # Полный ответ с fills
                }

                # Генерация подписи
                params['signature'] = self.ws._generate_signature(params)

                # Отправка запроса
                async with aiohttp.ClientSession() as session:
                    url = f'{self.ws.base_url}/order'
                    headers = {'X-MBX-APIKEY': self.ws.api_key}

                    async with session.post(url, headers=headers, params=params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.warning(f'⚠️ Buy order attempt {attempt + 1} failed: {error_text}')

                            if attempt < self.config['max_retries'] - 1:
                                await asyncio.sleep(self.config['retry_delay'])
                                continue

                            logger.error(f'❌ All buy order attempts failed for {symbol}')
                            return None

                        order_data = await response.json()
                        logger.info(f'✅ BUY order executed: {symbol} {formatted_quantity}')
                        logger.info(f'   Order status: {order_data.get("status")}')
                        logger.info(f'   Order ID: {order_data.get("orderId")}')

                        return order_data

            except Exception as e:
                logger.error(f'❌ Error creating buy order: {str(e)}')
                if attempt < self.config['max_retries'] - 1:
                    await asyncio.sleep(self.config['retry_delay'])
                else:
                    return None

        return None

    async def _save_trade_to_db_and_list(self, symbol: str, order_data: dict,
                                         current_price: float, quantity: float):
        """Сохранение сделки в БД и добавление в список активных"""
        try:
            # Получаем среднюю цену покупки из fills
            fills = order_data.get('fills', [])
            if fills:
                total_cost = 0
                total_qty = 0
                for fill in fills:
                    price = float(fill.get('price', 0))
                    qty = float(fill.get('qty', 0))
                    total_cost += price * qty
                    total_qty += qty

                avg_price = total_cost / total_qty if total_qty > 0 else current_price
            else:
                avg_price = current_price

            # Получаем exchange объект
            try:
                exchange = await Exchange.objects.aget(name='binance')
            except Exchange.DoesNotExist:
                exchange = await Exchange.objects.acreate(name='binance')

            # Создаем сделку в базе данных
            trade = await Trade.objects.acreate(
                user_id=self.user_id,
                symbol=symbol.upper(),
                quantity=quantity,
                buy_price=avg_price,
                buy_order_id=order_data.get('orderId'),
                status='active',
                exchange=exchange,
                target_profit_percent=self.config['target_profit_percent'],
                stop_loss_percent=self.config['stop_loss_percent']
            )

            # Добавляем сделку в активные трейды
            if not hasattr(self.ws, 'trades'):
                self.ws.trades = []

            trade_data = {
                'trade_id': trade.id,
                'symbol': symbol.lower(),
                'avg_buy_price': avg_price,
                'quantity': quantity,
                'user_id': self.user_id,
                'target_profit_percent': self.config['target_profit_percent'],
                'stop_loss_percent': self.config['stop_loss_percent'],
                'created_at': timezone.now().isoformat(),
                'closed': False
            }

            self.ws.trades.append(trade_data)

            logger.info(f"📝 Trade saved to DB. ID: {trade.id}")
            logger.info(f"   Active trades: {len(self.ws.trades)}")
            logger.info(f"   Price: ${avg_price:.2f}, Quantity: {quantity}")
            logger.info(f"   Target: {self.config['target_profit_percent']}%, Stop: {self.config['stop_loss_percent']}%")

        except Exception as e:
            logger.error(f"❌ Failed to save trade: {e}", exc_info=True)

    async def _check_usdt_balance(self, required_amount: float) -> bool:
        """Проверка баланса USDT"""
        try:
            if hasattr(self.ws, 'user_balance'):
                # Обновляем баланс
                await self.ws.get_user_balance()

                # Получаем USDT баланс
                usdt_info = self.ws.user_balance.get('USDT', {})
                free_usdt = float(usdt_info.get('free', 0))

                logger.info(f"💰 USDT balance: ${free_usdt:.2f}, Required: ${required_amount:.2f}")
                return free_usdt >= required_amount

        except Exception as e:
            logger.warning(f'⚠️ Error checking USDT balance: {e}')

        return False

    async def check_and_close_trades(self, market_data: dict) -> bool:
        """Основной метод проверки и закрытия сделок"""
        try:
            symbol = market_data.get('s', '').lower()
            market_price = float(market_data.get('p', 0))

            if market_price <= 0:
                return False

            # Проверяем активные сделки
            if not hasattr(self.ws, 'trades') or not self.ws.trades:
                return False

            # Находим сделки для этого символа
            trades_to_check = [
                trade for trade in self.ws.trades
                if trade.get('symbol', '').lower() == symbol
                   and not trade.get('closed', False)
            ]

            if not trades_to_check:
                return False

            logger.info(f"🔍 Checking {len(trades_to_check)} trades for {symbol.upper()} at ${market_price:.2f}")

            closed_any = False

            for trade in trades_to_check:
                trade_id = trade.get('trade_id')
                if not trade_id:
                    continue

                # Получаем данные из БД
                try:
                    db_trade = await Trade.objects.aget(id=trade_id)

                    # Если сделка уже закрыта, пропускаем
                    if db_trade.status not in ['active', 'open']:
                        continue

                except Trade.DoesNotExist:
                    logger.warning(f"⚠️ Trade {trade_id} not found in DB")
                    continue

                # Проверяем условия закрытия
                should_close, close_reason = await self._should_close_trade(
                    db_trade, market_price, trade
                )

                if should_close:
                    logger.info(f"🚀 Closing trade {trade_id}: {close_reason}")
                    success = await self._close_trade(db_trade, market_price, close_reason)

                    if success:
                        # Помечаем сделку как закрытую в списке
                        trade['closed'] = True
                        closed_any = True
                        logger.info(f"✅ Trade {trade_id} closed successfully")
                    else:
                        logger.error(f"❌ Failed to close trade {trade_id}")

            return closed_any

        except Exception as e:
            logger.error(f"❌ Error in check_and_close_trades: {e}", exc_info=True)
            return False

    async def _should_close_trade(self, db_trade, market_price: float, trade_data: dict) -> tuple:
        """Определяет, нужно ли закрывать сделку"""
        buy_price = float(trade_data.get('avg_buy_price', db_trade.buy_price))

        if buy_price <= 0:
            return False, "invalid_buy_price"

        # Рассчитываем прибыль
        profit_percent = ((market_price - buy_price) / buy_price) * 100

        # Получаем параметры из БД или конфига
        target_profit = db_trade.target_profit_percent or self.config['target_profit_percent']
        stop_loss = db_trade.stop_loss_percent or self.config['stop_loss_percent']

        logger.debug(f"📊 Trade {db_trade.id} analysis:")
        logger.debug(f"   Buy: ${buy_price:.2f}, Current: ${market_price:.2f}")
        logger.debug(f"   Profit: {profit_percent:.2f}%")
        logger.debug(f"   Target: {target_profit:.2f}%, Stop: -{stop_loss:.2f}%")

        # Проверяем условия
        if profit_percent <= -stop_loss:
            return True, f"stop_loss_{profit_percent:.2f}%"

        if profit_percent >= target_profit:
            return True, f"profit_{profit_percent:.2f}%"

        return False, ""

    async def _close_trade(self, trade, market_price: float, close_reason: str) -> bool:
        """Закрытие сделки с обработкой EXPIRED ордеров"""
        try:
            symbol = trade.symbol.upper()

            logger.info(f"💰 Closing {symbol}: {close_reason}")
            logger.info(f"   Quantity: {trade.quantity}, Buy price: ${trade.buy_price}")
            logger.info(f"   Market price: ${market_price:.2f}")

            # Создаем ордер на продажу
            order_result = await self.create_market_sell_order(symbol, trade.quantity)

            if not order_result:
                logger.error(f"❌ Failed to create sell order for {symbol}")
                return False

            # Проверяем статус ордера
            order_status = order_result.get('status')
            order_id = order_result.get('orderId')

            if order_status != 'FILLED':
                logger.warning(f"⚠️ Order {order_id} status: {order_status}")

                # Если ордер EXPIRED, пробуем IOC ордер
                if order_status == 'EXPIRED':
                    logger.info(f"🔄 Order expired, retrying with IOC order...")
                    order_result = await self._create_ioc_sell_order(symbol, trade.quantity, market_price)

                    if not order_result or order_result.get('status') != 'FILLED':
                        logger.error(f"❌ IOC order also failed for {symbol}")
                        return False

            # Получаем фактическую цену продажи
            sell_price = self._get_actual_sell_price(order_result)

            if sell_price <= 0:
                logger.error(f"❌ Invalid sell price: ${sell_price}")
                return False

            # Рассчитываем прибыль с учетом комиссии
            buy_commission = trade.buy_price * trade.quantity * self.config['commission_rate']
            sell_commission = sell_price * trade.quantity * self.config['commission_rate']

            gross_profit = (sell_price - trade.buy_price) * trade.quantity
            total_commission = buy_commission + sell_commission
            net_profit = gross_profit - total_commission
            net_profit_percent = (net_profit / (trade.buy_price * trade.quantity)) * 100

            # Определяем статус
            status = 'closed_profit' if net_profit > 0 else 'closed_loss'

            # Обновляем сделку в БД
            trade.sell_price = sell_price
            trade.actual_profit = net_profit
            trade.actual_profit_percent = net_profit_percent
            trade.commission_paid = total_commission
            trade.status = status
            trade.close_reason = close_reason
            trade.closed_at = timezone.now()
            await trade.asave()

            logger.info(f"📝 Trade updated:")
            logger.info(f"   Sell price: ${sell_price:.2f}")
            logger.info(f"   Net profit: ${net_profit:.2f} ({net_profit_percent:.2f}%)")
            logger.info(f"   Commission: ${total_commission:.2f}")
            logger.info(f"   Status: {status}")

            # Обновляем баланс
            if hasattr(self.ws, 'get_user_balance'):
                await self.ws.get_user_balance()

            # Удаляем из активных сделок
            await self._remove_from_active_trades(trade.id)

            return True

        except Exception as e:
            logger.error(f"❌ Error closing trade: {e}", exc_info=True)
            return False

    async def create_market_sell_order(self, symbol: str, quantity: float) -> Optional[Dict]:
        """Создание РЫНОЧНОГО ордера на продажу - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        if not self.config['auto_close_enabled']:
            logger.warning(f"⚠️ Auto close disabled for {symbol}")
            return None

        for attempt in range(self.config['max_retries']):
            try:
                await self.ws.sync_time()

                # Форматируем количество
                lot_size_info = await self._get_lot_size_info(symbol)
                formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)
                quantity_float = float(formatted_quantity)

                if quantity_float <= 0:
                    logger.error(f"❌ Invalid quantity: {quantity_float}")
                    return None

                # Правильные параметры для MARKET ордера
                params = {
                    'symbol': symbol.upper(),
                    'side': 'SELL',
                    'type': 'MARKET',
                    'quantity': formatted_quantity,
                    'timestamp': self.ws._get_timestamp(),
                    'recvWindow': 5000,
                    'newOrderRespType': 'FULL'  # Полный ответ с fills
                }

                params['signature'] = self.ws._generate_signature(params)

                logger.info(f"📤 Sending MARKET SELL order: {symbol} {quantity_float}")

                async with aiohttp.ClientSession() as session:
                    url = f'{self.ws.base_url}/order'
                    headers = {'X-MBX-APIKEY': self.ws.api_key}

                    async with session.post(url, headers=headers, params=params) as response:
                        if response.status == 200:
                            order_data = await response.json()
                            logger.info(f"✅ MARKET SELL order executed: {symbol} {quantity_float}")
                            logger.info(f"   Order ID: {order_data.get('orderId')}")
                            logger.info(f"   Status: {order_data.get('status')}")

                            # Проверяем, что ордер действительно исполнен
                            if order_data.get('status') == 'FILLED':
                                fills = order_data.get('fills', [])
                                if fills:
                                    avg_price = self._get_actual_sell_price(order_data)
                                    logger.info(f"   Filled: {order_data.get('executedQty')} @ avg ${avg_price:.2f}")
                            else:
                                logger.warning(f"⚠️ Order not filled: {order_data.get('status')}")

                            return order_data
                        else:
                            error_text = await response.text()
                            logger.error(f'❌ Sell order failed: {error_text}')

                            if attempt < self.config['max_retries'] - 1:
                                await asyncio.sleep(self.config['retry_delay'])
                                continue

                            return None

            except Exception as e:
                logger.error(f"❌ Error creating sell order: {e}")
                if attempt < self.config['max_retries'] - 1:
                    await asyncio.sleep(self.config['retry_delay'])
                else:
                    return None

        return None

    async def _create_ioc_sell_order(self, symbol: str, quantity: float, current_price: float) -> Optional[Dict]:
        """Создание лимитного ордера с IOC для гарантированного исполнения"""
        try:
            # Используем лимитный ордер с ценой ниже рынка для быстрого исполнения
            limit_price = current_price * 0.995  # 0.5% ниже рынка

            lot_size_info = await self._get_lot_size_info(symbol)
            formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)
            formatted_price = self._format_price(limit_price, lot_size_info)

            params = {
                'symbol': symbol.upper(),
                'side': 'SELL',
                'type': 'LIMIT',
                'timeInForce': 'IOC',  # Immediate Or Cancel
                'quantity': formatted_quantity,
                'price': formatted_price,
                'timestamp': self.ws._get_timestamp(),
                'recvWindow': 5000,
                'newOrderRespType': 'FULL'
            }

            params['signature'] = self.ws._generate_signature(params)

            logger.info(f"📤 Sending IOC LIMIT SELL: {symbol} {quantity} @ ${limit_price:.2f}")

            async with aiohttp.ClientSession() as session:
                url = f'{self.ws.base_url}/order'
                headers = {'X-MBX-APIKEY': self.ws.api_key}

                async with session.post(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        order_data = await response.json()
                        logger.info(f"✅ IOC LIMIT order result: {order_data.get('status')}")
                        return order_data
                    else:
                        error_text = await response.text()
                        logger.error(f"❌ IOC order failed: {error_text}")
                        return None

        except Exception as e:
            logger.error(f"❌ IOC order error: {e}")
            return None

    def _format_price(self, price: float, lot_size_info: Dict) -> str:
        """Форматирование цены по правилам биржи"""
        try:
            # Получаем TICK_SIZE фильтр
            tick_size = 0.01  # default

            if 'filters' in lot_size_info:
                for f in lot_size_info['filters']:
                    if f.get('filterType') == 'PRICE_FILTER':
                        tick_size = float(f.get('tickSize', 0.01))
                        break

            # Округляем до ближайшего tick size
            if tick_size > 0:
                steps = round(price / tick_size)
                price = steps * tick_size

            # Определяем точность
            if tick_size >= 1:
                precision = 0
            elif tick_size >= 0.1:
                precision = 1
            elif tick_size >= 0.01:
                precision = 2
            elif tick_size >= 0.001:
                precision = 3
            else:
                precision = 8

            return f"{price:.{precision}f}"

        except Exception as e:
            logger.warning(f"⚠️ Error formatting price: {e}")
            return f"{price:.2f}"

    def _get_actual_sell_price(self, order_data: Dict) -> float:
        """Получает фактическую цену продажи из ордера"""
        try:
            fills = order_data.get('fills', [])
            if fills:
                total_qty = 0
                total_value = 0
                for fill in fills:
                    qty = float(fill.get('qty', 0))
                    price = float(fill.get('price', 0))
                    total_qty += qty
                    total_value += qty * price

                if total_qty > 0:
                    return total_value / total_qty

            # Если нет fills, используем цену из ордера
            return float(order_data.get('price', 0))
        except:
            return 0.0

    async def _get_current_price(self, symbol: str) -> float:
        """Получение текущей цены"""
        cache_key = f"price_{symbol}"

        # Проверка кеша
        if cache_key in self._cache['symbol_info']:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < 5:
                return self._cache['symbol_info'][cache_key]

        try:
            async with aiohttp.ClientSession() as session:
                url = f'{self.ws.base_url}/ticker/price'
                params = {'symbol': symbol.upper()}

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = float(data.get('price', 0))

                        # Сохраняем в кеш
                        self._cache['symbol_info'][cache_key] = price
                        self._cache_timestamps[cache_key] = time.time()

                        return price
        except Exception as e:
            logger.warning(f"⚠️ Failed to get price for {symbol}: {e}")

        return 0.0

    async def _get_lot_size_info(self, symbol: str) -> Dict:
        """Получение информации о лот-сайзе"""
        cache_key = f"lot_{symbol}"

        if cache_key in self._cache['lot_size_info']:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < self.config['cache_ttl']:
                return self._cache['lot_size_info'][cache_key]

        try:
            async with aiohttp.ClientSession() as session:
                url = f'{self.ws.base_url}/exchangeInfo'
                params = {'symbol': symbol.upper()}

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        symbol_info = data.get('symbols', [{}])[0]
                        filters = symbol_info.get('filters', [])

                        lot_size = next(
                            (f for f in filters if f.get('filterType') == 'LOT_SIZE'),
                            {}
                        )

                        info = {
                            'minQty': float(lot_size.get('minQty', 0)),
                            'maxQty': float(lot_size.get('maxQty', 0)),
                            'stepSize': float(lot_size.get('stepSize', 0.00000001)),
                            'precision': self._get_precision_from_step(lot_size.get('stepSize', '0.00000001'))
                        }

                        self._cache['lot_size_info'][cache_key] = info
                        self._cache_timestamps[cache_key] = time.time()

                        return info
        except Exception as e:
            logger.warning(f"⚠️ Failed to get lot size for {symbol}: {e}")

        # Возвращаем значения по умолчанию
        return {
            'minQty': 0.00000001,
            'maxQty': 90000000,
            'stepSize': 0.00000001,
            'precision': 8
        }

    def _get_precision_from_step(self, step_size: str) -> int:
        """Определяет точность из шага"""
        try:
            if '.' in step_size:
                return len(step_size.split('.')[1].rstrip('0'))
            return 0
        except:
            return 8

    def _format_quantity_with_rules(self, quantity: float, lot_size_info: Dict) -> str:
        """Форматирует количество по правилам биржи"""
        step = lot_size_info.get('stepSize', 0.00000001)
        precision = lot_size_info.get('precision', 8)

        if step > 0:
            # Округляем до ближайшего шага
            steps = round(quantity / step)
            quantity = steps * step

        # Форматируем с нужной точностью
        return f"{quantity:.{precision}f}".rstrip('0').rstrip('.')

    async def _remove_from_active_trades(self, trade_id: int):
        """Удаляет сделку из активного списка"""
        if hasattr(self.ws, 'trades'):
            initial_count = len(self.ws.trades)
            self.ws.trades = [
                t for t in self.ws.trades
                if t.get('trade_id') != trade_id
            ]
            final_count = len(self.ws.trades)

            if initial_count != final_count:
                logger.info(f"🗑️ Removed trade {trade_id} from active list")
            else:
                logger.warning(f"⚠️ Trade {trade_id} not found in active list")

    def _clear_cache_for_symbol(self, symbol: str):
        """Очищает кеш для символа"""
        keys_to_remove = []

        for key in list(self._cache['lot_size_info'].keys()):
            if symbol.upper() in key:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del self._cache['lot_size_info'][key]
            if key in self._cache_timestamps:
                del self._cache_timestamps[key]

        price_key = f"price_{symbol.lower()}"
        if price_key in self._cache['symbol_info']:
            del self._cache['symbol_info'][price_key]
            if price_key in self._cache_timestamps:
                del self._cache_timestamps[price_key]