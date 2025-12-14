import logging
import sys
import os
import time
import asyncio
from typing import Dict, Optional
from django.utils import timezone

# Добавляем путь к Django проекту
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.append(project_root)

# Настраиваем Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

try:
    import django

    django.setup()
    from app.models import Trade, Exchange  # ✅ Прямой импорт
    from django.conf import settings
except ImportError as e:
    logging.error(f"Failed to setup Django: {e}")
    raise

logger = logging.getLogger(__name__)


class TradingService:
    def __init__(self, binance_ws_instance):
        self.ws = binance_ws_instance
        self.user_id = binance_ws_instance.user_id

        # Загружаем конфигурацию из TRADING_CONFIG
        trading_config = getattr(settings, 'TRADING_CONFIG', {})

        # Конфигурация с приоритетом: Django Settings -> TRADING_CONFIG -> значения по умолчанию
        self.config = {
            'target_profit_percent': getattr(settings, 'TARGET_PROFIT_PERCENT',
                                             trading_config.get('DEFAULT_TARGET_PROFIT', 1.0)),
            'stop_loss_percent': getattr(settings, 'STOP_LOSS_PERCENT',
                                         trading_config.get('DEFAULT_STOP_LOSS', 0.5)),
            'min_trade_amount_usd': trading_config.get('MIN_TRADE_AMOUNT_USD', 10.0),
            'commission_rate': trading_config.get('COMMISSION_RATE', 0.001),
            'auto_close_enabled': trading_config.get('AUTO_CLOSE_ENABLED', True),
            'max_retries': getattr(settings, 'API_MAX_RETRIES', 3),
            'retry_delay': getattr(settings, 'API_RETRY_DELAY', 1.0),
            'cache_ttl': getattr(settings, 'TRADE_CACHE_TTL', 60),
        }

        logger.info(f"📊 TradingService initialized with config:")
        logger.info(f"   Target profit: {self.config['target_profit_percent']}%")
        logger.info(f"   Stop loss: {self.config['stop_loss_percent']}%")
        logger.info(f"   Min trade amount: ${self.config['min_trade_amount_usd']}")
        logger.info(f"   Commission rate: {self.config['commission_rate'] * 100}%")
        logger.info(f"   Auto close: {self.config['auto_close_enabled']}")

        # Кеш для данных
        self._cache = {
            'balance_check': {},
            'lot_size_info': {},
            'active_trades': {},
            'symbol_info': {}
        }
        self._cache_timestamps = {}

    async def create_market_sell_order(self, symbol: str, quantity: float, is_closing_position: bool = True) -> \
    Optional[Dict]:
        """Создание рыночного ордера на продажу

        Args:
            symbol: Торговая пара
            quantity: Количество для продажи
            is_closing_position: True если закрываем позицию, False если новая продажа
        """
        if not self.config['auto_close_enabled']:
            logger.warning(f"⚠️ Auto close disabled, skipping sell order for {symbol}")
            return None

        for attempt in range(self.config['max_retries']):
            try:
                # Синхронизируем время
                await self.ws.sync_time()

                # Получаем информацию о лот-сайзе для символа
                lot_size_info = await self._get_lot_size_info(symbol)
                formatted_quantity = self._format_quantity_with_rules(quantity, lot_size_info)

                # Рассчитываем комиссию
                adjusted_quantity = await self._adjust_for_commission(
                    symbol,
                    float(formatted_quantity),
                    'SELL'
                )

                # Форматируем с учетом корректировки
                formatted_quantity = self._format_quantity_with_rules(adjusted_quantity, lot_size_info)

                # Конвертируем в число для расчетов
                quantity_float = float(formatted_quantity)

                # Получаем текущую цену для проверки суммы
                current_price = await self._get_current_price(symbol)
                trade_amount_usd = quantity_float * current_price

                logger.info(f"💰 Order amount calculation:")
                logger.info(f"   Quantity: {quantity_float}")
                logger.info(f"   Current price: ${current_price}")
                logger.info(f"   Trade amount: ${trade_amount_usd:.2f}")
                logger.info(f"   Minimum required: ${self.config['min_trade_amount_usd']}")
                logger.info(f"   Is closing position: {is_closing_position}")

                # Проверяем минимальную сумму сделки ТОЛЬКО для новых продаж
                if not is_closing_position and trade_amount_usd < self.config['min_trade_amount_usd']:
                    logger.warning(f"⚠️ Trade amount ${trade_amount_usd:.2f} below minimum "
                                   f"${self.config['min_trade_amount_usd']}, skipping")
                    return None

                # Если закрываем позицию и сумма маленькая, все равно закрываем
                if is_closing_position and trade_amount_usd < self.config['min_trade_amount_usd']:
                    logger.warning(f"⚠️ Trade amount ${trade_amount_usd:.2f} below minimum "
                                   f"${self.config['min_trade_amount_usd']}, but closing position anyway")

                # Проверяем, что количество больше 0 после корректировок
                if quantity_float <= 0:
                    logger.error(f"❌ Invalid quantity after adjustments: {quantity_float}")
                    return None

                # Подготовка параметров
                params = {
                    'symbol': symbol.upper(),
                    'side': 'SELL',
                    'type': 'MARKET',
                    'quantity': formatted_quantity,
                    'timestamp': self.ws._get_timestamp(),
                    'recvWindow': 5000
                }

                # Генерация подписи
                params['signature'] = self.ws._generate_signature(params)

                # Отправка запроса
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    url = f'{self.ws.base_url}/order'
                    headers = {'X-MBX-APIKEY': self.ws.api_key}

                    async with session.post(url, headers=headers, params=params) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.warning(f'⚠️ Sell order attempt {attempt + 1} failed: {error_text}')

                            if attempt < self.config['max_retries'] - 1:
                                await asyncio.sleep(self.config['retry_delay'])
                                continue

                            logger.error(f'❌ All sell order attempts failed for {symbol}')
                            return None

                        order_data = await response.json()

                        # Учитываем комиссию в результатах
                        order_data = await self._apply_commission_to_order(order_data, 'SELL')

                        logger.info(f'✅ SELL order executed: {order_data}')
                        logger.info(f'   Commission applied: {self.config["commission_rate"] * 100}%')
                        logger.info(f'   Amount: ${trade_amount_usd:.2f}')

                        return order_data

            except aiohttp.ClientError as e:
                logger.warning(f'⚠️ Network error on attempt {attempt + 1}: {e}')
                if attempt < self.config['max_retries'] - 1:
                    await asyncio.sleep(self.config['retry_delay'])
                    continue
                logger.error(f'❌ Network error after all attempts: {e}')
            except Exception as e:
                logger.error(f'❌ Error creating sell order: {str(e)}', exc_info=True)
                return None

        return None

    async def _adjust_for_commission(self, symbol: str, quantity: float, side: str) -> float:
        """Корректировка количества с учетом комиссии"""
        if self.config['commission_rate'] <= 0:
            return quantity

        # Для SELL ордера уменьшаем количество на комиссию
        if side == 'SELL':
            adjusted = quantity * (1 - self.config['commission_rate'])
            logger.debug(f"   Adjusted {quantity} -> {adjusted} for {side} commission")
            return adjusted
        # Для BUY ордера (если будет реализован) можно учесть по-другому
        return quantity

    async def _apply_commission_to_order(self, order_data: Dict, side: str) -> Dict:
        """Применение комиссии к данным ордера"""
        if self.config['commission_rate'] <= 0:
            return order_data

        try:
            # Обновляем информацию о комиссии в fills
            fills = order_data.get('fills', [])
            for fill in fills:
                # Учитываем, что Binance уже взимает комиссию, но мы ее логируем
                commission = float(fill.get('commission', 0))
                commission_asset = fill.get('commissionAsset', '')

                # Логируем нашу внутреннюю комиссию
                if side == 'SELL':
                    qty = float(fill.get('qty', 0))
                    our_commission = qty * self.config['commission_rate']
                    fill['our_commission'] = our_commission
                    fill['our_commission_asset'] = 'USDT' if 'USDT' in order_data['symbol'] else 'BNB'

            # Добавляем информацию о нашей комиссии в корень ордера
            order_data['our_commission_rate'] = self.config['commission_rate']
            order_data['our_commission_applied'] = True

        except Exception as e:
            logger.warning(f"⚠️ Error applying commission to order: {e}")

        return order_data

    async def close_trade_with_profit(self, trade_data: dict, market_price: float,
                                      close_reason: str = "profit") -> bool:
        """Закрытие сделки при достижении профита или стоп-лосса"""
        try:
            symbol = trade_data.get('symbol').upper()
            trade_id = trade_data.get('trade_id')

            if not trade_id:
                logger.error('❌ Trade ID is required')
                return False

            if not self.config['auto_close_enabled']:
                logger.info(f"⚠️ Auto close disabled, not closing trade {trade_id}")
                return False

            # Получаем объект сделки из БД
            try:
                trade = await Trade.objects.aget(id=trade_id)
            except Trade.DoesNotExist:
                logger.error(f'❌ Trade with ID {trade_id} not found in database')
                return False

            # Рассчитываем сумму сделки для информации
            trade_amount_usd = trade.quantity * market_price

            logger.info(f"💰 Closing trade calculation for {symbol}:")
            logger.info(f"   Trade ID: {trade.id}")
            logger.info(f"   Quantity: {trade.quantity}")
            logger.info(f"   Buy price: ${trade.buy_price}")
            logger.info(f"   Current price: ${market_price}")
            logger.info(f"   Trade amount: ${trade_amount_usd:.2f}")

            # Рассчитываем профит/убыток
            profit_percent = ((market_price - trade.buy_price) / trade.buy_price) * 100

            # Рассчитываем комиссию
            commission_amount = trade.quantity * market_price * self.config['commission_rate']
            net_profit_percent = profit_percent - (self.config['commission_rate'] * 100 * 2)

            logger.info(f'🎯 Closing trade {symbol}: {close_reason}')
            logger.info(f'   Market price: ${market_price}')
            logger.info(f'   Gross profit: {profit_percent:.2f}%')
            logger.info(f'   Commission: ${commission_amount:.2f} ({self.config["commission_rate"] * 100}%)')
            logger.info(f'   Net profit: {net_profit_percent:.2f}%')

            # Создаем ордер на продажу с флагом is_closing_position=True
            order_result = await self.create_market_sell_order(
                symbol=symbol,
                quantity=trade.quantity,
                is_closing_position=True  # ⚠️ Важно: указываем что это закрытие позиции
            )

            if order_result:
                # Получаем цену продажи из ордера
                fills = order_result.get('fills', [])
                sell_price = float(fills[0].get('price', market_price)) if fills else market_price

                # Рассчитываем фактический профит с учетом комиссии
                gross_profit = (sell_price - trade.buy_price) * trade.quantity
                commission_total = trade.quantity * sell_price * self.config['commission_rate']
                net_profit = gross_profit - commission_total
                net_profit_percent = ((sell_price * (1 - self.config['commission_rate']) -
                                       trade.buy_price * (1 + self.config['commission_rate'])) /
                                      (trade.buy_price * (1 + self.config['commission_rate']))) * 100

                # Определяем статус закрытия
                if "profit" in close_reason and net_profit > 0:
                    status = 'closed_profit'
                elif "stop_loss" in close_reason:
                    status = 'closed_loss'
                else:
                    status = 'closed_manual'

                # Обновляем сделку в БД
                trade.sell_price = sell_price
                trade.actual_profit = net_profit  # Сохраняем чистую прибыль
                trade.actual_profit_percent = net_profit_percent
                trade.commission_paid = commission_total
                trade.status = status
                trade.close_reason = close_reason
                trade.closed_at = timezone.now()
                await trade.asave()

                logger.info(f'📝 Trade updated in database:')
                logger.info(f'   Sell price: ${sell_price}')
                logger.info(f'   Net profit: ${net_profit:.2f}')
                logger.info(f'   Net profit %: {net_profit_percent:.2f}%')
                logger.info(f'   Commission: ${commission_total:.2f}')
                logger.info(f'   Status: {status}')

                # Обновляем баланс
                await self.ws.get_user_balance()

                # Обновляем метрики
                await self._update_trading_metrics(net_profit, close_reason, commission_total)

                logger.info(f'💰 Trade closed: ${net_profit:.2f} ({net_profit_percent:.2f}%) net')

                # Удаляем сделку из активных трейдов
                if hasattr(self.ws, 'trades') and self.ws.trades:
                    before_count = len(self.ws.trades)
                    self.ws.trades = [
                        t for t in self.ws.trades
                        if not (t.get('trade_id') == trade_id)
                    ]
                    after_count = len(self.ws.trades)

                    if before_count > after_count:
                        symbol_lower = symbol.lower()
                        remaining_trades_for_symbol = [
                            t for t in self.ws.trades
                            if t.get('symbol', '').lower() == symbol_lower
                        ]

                        if not remaining_trades_for_symbol:
                            # Отписываемся от рыночных данных для этого символа
                            if hasattr(self.ws, 'unsubscribe_from_symbol'):
                                await self.ws.unsubscribe_from_symbol(symbol_lower)
                                logger.info(f"📭 Stopped listening to {symbol} (no active trades)")

                        logger.info(f'🗑️ Removed trade from active list: {before_count} -> {after_count}')
                    else:
                        logger.warning(f'⚠️ Trade {trade_id} not found in active list')

                # Очищаем кеш для этого символа
                self._clear_symbol_cache(symbol)

                return True
            else:
                logger.error(f'❌ Failed to execute sell order for {symbol}')

                # Если не удалось закрыть из-за маленькой суммы, помечаем как закрытую вручную
                if trade_amount_usd < self.config['min_trade_amount_usd']:
                    logger.warning(f"⚠️ Trade amount too small, marking as closed manually")
                    trade.status = 'closed_manual'
                    trade.close_reason = 'amount_too_small'
                    trade.closed_at = timezone.now()
                    await trade.asave()

                    # Удаляем из активных
                    if hasattr(self.ws, 'trades') and self.ws.trades:
                        self.ws.trades = [t for t in self.ws.trades if t.get('trade_id') != trade_id]

                    return True

                return False

        except Exception as e:
            logger.error(f'❌ Error closing trade: {str(e)}', exc_info=True)
            return False

    async def check_and_close_trades(self, market_data: dict) -> bool:
        """Проверка и закрытие сделок с учетом конфигурации"""
        try:
            symbol = market_data.get('s', '').lower()
            market_price = float(market_data.get('p', 0))

            logger.debug(f"🔍 TRADING_SERVICE: Checking {symbol.upper()} at ${market_price}")

            # Проверяем, включено ли автоматическое закрытие
            if not self.config['auto_close_enabled']:
                logger.debug(f"⚠️ Auto close disabled, skipping check for {symbol.upper()}")
                return False

            # Проверяем, есть ли активные сделки
            if not hasattr(self.ws, 'trades') or not self.ws.trades:
                logger.debug(f"📭 No active trades found for {symbol.upper()}")
                return False

            # Ищем сделки для этого символа
            matching_trades = []
            for i, trade in enumerate(self.ws.trades):
                trade_symbol = trade.get('symbol', '').lower()
                if trade_symbol == symbol:
                    matching_trades.append((i, trade))

            if not matching_trades:
                logger.debug(f"⚠️ No active trades for {symbol.upper()} in list")
                return False

            # Проверяем каждую сделку
            closed_any = False
            for trade_index, trade in matching_trades:
                trade_id = trade.get('trade_id')

                if not trade_id:
                    logger.error(f"❌ Trade without ID found at index {trade_index}")
                    continue

                # Получаем данные сделки из БД для проверки параметров
                try:
                    db_trade = await Trade.objects.aget(id=trade_id)

                    # Если сделка уже закрыта в БД, удаляем из активных
                    if db_trade.status in ['closed_profit', 'closed_loss', 'closed_manual']:
                        logger.info(f"📄 Trade {trade_id} already closed in DB, removing from active list...")
                        self.ws.trades = [t for t in self.ws.trades if t.get('trade_id') != trade_id]
                        continue

                except Trade.DoesNotExist:
                    logger.warning(f"⚠️ Trade {trade_id} not found in DB")
                    continue

                # Проверяем данные сделки
                buy_price = float(trade.get('avg_buy_price', 0))
                quantity = float(trade.get('quantity', 0))

                if buy_price <= 0:
                    logger.error(f"❌ Invalid buy price for trade {trade_id}: ${buy_price}")
                    continue

                # Рассчитываем текущий PnL
                profit_percent = ((market_price - buy_price) / buy_price) * 100

                # Учитываем комиссию в расчетах
                commission_impact = self.config['commission_rate'] * 100 * 2  # Вход и выход
                net_profit_percent = profit_percent - commission_impact

                # Получаем параметры закрытия из БД или конфига
                target_profit = db_trade.target_profit_percent if db_trade.target_profit_percent is not None else \
                self.config['target_profit_percent']
                stop_loss = db_trade.stop_loss_percent if db_trade.stop_loss_percent is not None else self.config[
                    'stop_loss_percent']

                # Исправленная логика проверки условий закрытия
                should_close = False
                close_reason = ""

                # Проверяем стоп-лосс (с учетом комиссии)
                stop_loss_with_commission = stop_loss + commission_impact
                if profit_percent <= -stop_loss_with_commission:
                    should_close = True
                    close_reason = f"stop_loss_{profit_percent:.2f}%"
                    logger.info(f"🛑 STOP LOSS triggered: {profit_percent:.2f}% <= -{stop_loss_with_commission:.2f}% "
                                f"(stop loss {stop_loss}% + commission {commission_impact:.2f}%)")

                # Затем проверяем тейк-профит (с учетом комиссии)
                elif profit_percent >= target_profit + commission_impact:
                    should_close = True
                    close_reason = f"profit_{profit_percent:.2f}%"
                    logger.info(f"✅ TAKE PROFIT triggered: {profit_percent:.2f}% >= "
                                f"{target_profit + commission_impact:.2f}% "
                                f"(target {target_profit}% + commission {commission_impact:.2f}%)")

                # Подробное логирование условий
                logger.debug(f"🎯 Conditions for trade {trade_id}:")
                logger.debug(f"   Current PnL: {profit_percent:.2f}%")
                logger.debug(f"   Net PnL (after commission): {net_profit_percent:.2f}%")
                logger.debug(f"   Target profit: {target_profit}%")
                logger.debug(f"   Stop loss: -{stop_loss}%")
                logger.debug(f"   Commission impact: {commission_impact:.2f}%")
                logger.debug(f"   Should close: {should_close}")

                if should_close:
                    logger.info(f"🚀 CLOSING TRADE {trade_id}: {close_reason}")

                    # Закрываем сделку
                    success = await self.close_trade_with_profit(trade, market_price, close_reason)

                    if success:
                        closed_any = True
                        logger.info(f"✅ Trade {trade_id} closed successfully")
                    else:
                        logger.error(f"❌ Failed to close trade {trade_id}")
                else:
                    logger.debug(f"⏳ Keeping trade {trade_id}: PnL {profit_percent:.2f}%, "
                                 f"net {net_profit_percent:.2f}% after commission")

            return closed_any

        except Exception as e:
            logger.error(f"❌ Error in check_and_close_trades: {e}", exc_info=True)
            return False

    async def _get_current_price(self, symbol: str) -> float:
        """Получение текущей цены символа"""
        cache_key = f"price_{symbol}"

        # Проверяем кеш
        if cache_key in self._cache['symbol_info']:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < 5:  # Кеш цены на 5 секунд
                return self._cache['symbol_info'][cache_key]

        try:
            import aiohttp
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
            logger.warning(f'⚠️ Failed to get current price: {e}')

        return 0.0

    async def validate_trade_amount(self, symbol: str, quantity: float, price: float) -> bool:
        """Проверка минимальной суммы сделки"""
        trade_amount_usd = quantity * price

        if trade_amount_usd < self.config['min_trade_amount_usd']:
            logger.warning(f"❌ Trade amount ${trade_amount_usd:.2f} below minimum "
                           f"${self.config['min_trade_amount_usd']}")
            return False

        logger.debug(f"✓ Trade amount ${trade_amount_usd:.2f} meets minimum requirement")
        return True

    async def calculate_position_size(self, symbol: str, risk_amount: float, stop_loss_percent: float) -> float:
        """Расчет размера позиции на основе риска"""
        try:
            current_price = await self._get_current_price(symbol)
            if current_price <= 0:
                return 0.0

            # Расчет позиции: риск / (цена * стоп-лосс %)
            position_size = risk_amount / (current_price * stop_loss_percent / 100)

            # Получаем правила лот-сайза
            lot_size_info = await self._get_lot_size_info(symbol)

            # Округляем до правильного шага
            step_size = lot_size_info.get('stepSize', 0.00000001)
            if step_size > 0:
                steps = round(position_size / step_size)
                position_size = steps * step_size

            # Проверяем минимальное/максимальное количество
            min_qty = lot_size_info.get('minQty', 0)
            max_qty = lot_size_info.get('maxQty', float('inf'))

            if position_size < min_qty:
                position_size = min_qty
            elif position_size > max_qty:
                position_size = max_qty

            # Проверяем минимальную сумму
            trade_amount = position_size * current_price
            if trade_amount < self.config['min_trade_amount_usd']:
                logger.warning(f"⚠️ Calculated position ${trade_amount:.2f} below minimum, adjusting...")
                position_size = self.config['min_trade_amount_usd'] / current_price

            logger.info(f"📏 Position size calculated: {position_size:.8f} {symbol.replace('USDT', '')} "
                        f"(${trade_amount:.2f}) with risk ${risk_amount}")

            return position_size

        except Exception as e:
            logger.error(f"❌ Error calculating position size: {e}")
            return 0.0

    async def _update_trading_metrics(self, net_profit: float, close_reason: str, commission: float):
        """Обновление метрик торговли с учетом комиссии"""
        try:
            metrics_data = {
                'user_id': self.user_id,
                'net_profit': net_profit,
                'commission': commission,
                'gross_profit': net_profit + commission,
                'close_reason': close_reason,
                'timestamp': time.time(),
                'trade_count': len(self.ws.trades) if hasattr(self.ws, 'trades') else 0,
                'config': {
                    'target_profit': self.config['target_profit_percent'],
                    'stop_loss': self.config['stop_loss_percent'],
                    'commission_rate': self.config['commission_rate']
                }
            }

            logger.info(f"📈 Trading metrics: {metrics_data}")

            # Здесь можно добавить отправку метрик во внешнюю систему
            # await self._send_metrics_to_monitoring(metrics_data)

        except Exception as e:
            logger.warning(f'⚠️ Error updating metrics: {e}')

    # Остальные вспомогательные методы остаются без изменений
    async def _get_lot_size_info(self, symbol: str) -> Dict:
        """Получение информации о правилах торговли для символа"""
        cache_key = f"lot_size_{symbol}"

        # Проверяем кеш
        if cache_key in self._cache['lot_size_info']:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < self.config['cache_ttl']:
                return self._cache['lot_size_info'][cache_key]

        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                url = f'{self.ws.base_url}/exchangeInfo'
                params = {'symbol': symbol.upper()}

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()

                        filters = data.get('symbols', [{}])[0].get('filters', [])
                        lot_size_filter = next(
                            (f for f in filters if f['filterType'] == 'LOT_SIZE'),
                            {}
                        )

                        info = {
                            'minQty': float(lot_size_filter.get('minQty', 0)),
                            'maxQty': float(lot_size_filter.get('maxQty', 0)),
                            'stepSize': float(lot_size_filter.get('stepSize', 0)),
                            'precision': self._get_precision_from_step(lot_size_filter.get('stepSize', '0'))
                        }

                        self._cache['lot_size_info'][cache_key] = info
                        self._cache_timestamps[cache_key] = time.time()

                        return info
        except Exception as e:
            logger.warning(f'⚠️ Failed to get lot size info: {e}')

        return self._get_default_lot_size(symbol)

    def _get_default_lot_size(self, symbol: str) -> Dict:
        """Получение правил по умолчанию для основных пар"""
        default_rules = {
            'BTCUSDT': {'minQty': 0.000001, 'maxQty': 1000, 'stepSize': 0.000001, 'precision': 6},
            'ETHUSDT': {'minQty': 0.00001, 'maxQty': 10000, 'stepSize': 0.00001, 'precision': 5},
            'BNBUSDT': {'minQty': 0.001, 'maxQty': 90000, 'stepSize': 0.001, 'precision': 3},
            'ADAUSDT': {'minQty': 1, 'maxQty': 900000, 'stepSize': 1, 'precision': 1},
            'DOGEUSDT': {'minQty': 1, 'maxQty': 9000000, 'stepSize': 1, 'precision': 0},
        }
        return default_rules.get(symbol.upper(),
                                 {'minQty': 0.00000001, 'maxQty': 90000000, 'stepSize': 0.00000001, 'precision': 8})

    def _get_precision_from_step(self, step_size: str) -> int:
        """Получение точности из шага размера лота"""
        try:
            if '.' in step_size:
                return len(step_size.split('.')[1].rstrip('0'))
            else:
                step = int(step_size)
                if step == 1:
                    return 0
                count = 0
                while step % 10 == 0:
                    step //= 10
                    count += 1
                return -count
        except:
            return 8

    def _format_quantity_with_rules(self, quantity: float, lot_size_info: Dict) -> str:
        """Форматирование количества согласно правилам лот-сайза"""
        precision = lot_size_info.get('precision', 8)
        step_size = lot_size_info.get('stepSize', 0.00000001)

        if step_size > 0:
            steps = round(quantity / step_size)
            quantity = steps * step_size

        format_str = f"{{:.{precision}f}}"
        formatted = format_str.format(quantity)
        formatted = formatted.rstrip('0').rstrip('.')

        return formatted

    async def _check_user_has_balance_for_symbol(self, symbol: str) -> bool:
        """Проверка наличия баланса для символа"""
        cache_key = f"balance_{symbol}_{self.user_id}"

        if cache_key in self._cache['balance_check']:
            cache_time = self._cache_timestamps.get(cache_key, 0)
            if time.time() - cache_time < self.config['cache_ttl']:
                return self._cache['balance_check'][cache_key]

        try:
            if hasattr(self.ws, 'get_user_balance'):
                balance_data = await self.ws.get_user_balance()

                base_asset = symbol.upper().replace('USDT', '')

                for asset in balance_data:
                    if asset['asset'] == base_asset and float(asset['free']) > 0:
                        self._cache['balance_check'][cache_key] = True
                        self._cache_timestamps[cache_key] = time.time()
                        return True

            self._cache['balance_check'][cache_key] = False
            self._cache_timestamps[cache_key] = time.time()
            return False

        except Exception as e:
            logger.warning(f'⚠️ Error checking balance for {symbol}: {e}')
            return False

    def _clear_symbol_cache(self, symbol: str):
        """Очистка кеша для символа"""
        symbol_upper = symbol.upper()

        keys_to_delete = []
        for key in list(self._cache['lot_size_info'].keys()):
            if symbol_upper in key:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self._cache['lot_size_info'][key]
            if key in self._cache_timestamps:
                del self._cache_timestamps[key]

        balance_key = f"balance_{symbol}_{self.user_id}"
        if balance_key in self._cache['balance_check']:
            del self._cache['balance_check'][balance_key]
        if balance_key in self._cache_timestamps:
            del self._cache_timestamps[balance_key]

    async def cleanup_cache(self):
        """Очистка устаревшего кеша"""
        current_time = time.time()
        expired_keys = []

        for cache_name, cache in self._cache.items():
            for key in list(cache.keys()):
                cache_time = self._cache_timestamps.get(key, 0)
                if current_time - cache_time > self.config['cache_ttl']:
                    expired_keys.append((cache_name, key))

        for cache_name, key in expired_keys:
            del self._cache[cache_name][key]
            if key in self._cache_timestamps:
                del self._cache_timestamps[key]

        if expired_keys:
            logger.info(f"🧹 Cleaned up {len(expired_keys)} expired cache entries")