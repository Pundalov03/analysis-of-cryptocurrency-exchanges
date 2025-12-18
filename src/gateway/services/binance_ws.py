# binance_ws.py - ДОБАВИМ МЕТОДЫ ДЛЯ ОБРАБОТКИ EXPIRED
import aiohttp
import json
import asyncio
import websockets
import logging
import time
import hmac
import hashlib
import urllib.parse
import sys
import os

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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceWebsocket(object):
    def __init__(self, user_id: int, api_key: str, secret_key: str, trades: list, testnet: bool = True):
        self.user_id = user_id
        self.api_key = api_key
        self.secret_key = secret_key
        self.listen_key = None
        self.trades = trades
        self.commission = 0.99
        self.time_offset = 0
        self._tasks = []
        self.trading_service = None

        if testnet:
            self.base_url = 'https://testnet.binance.vision/api/v3'
            self.ws_url = 'wss://stream.testnet.binance.vision/ws'
            self.ws_stream_url = 'wss://stream.testnet.binance.vision/stream'
        else:
            self.base_url = 'https://api.binance.com/api/v3'
            self.ws_url = 'wss://stream.binance.com:9443/ws'
            self.ws_stream_url = 'wss://stream.binance.com:9443/stream'

        self.connection = None
        self.active_streams = set()
        self.market_connection = None
        self.is_connected = False
        self.user_balance = {}
        self.symbol_to_listen = []

    async def get_server_time(self):
        try:
            async with aiohttp.ClientSession() as session:
                url = f'{self.base_url}/time'
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['serverTime']
                    else:
                        logger.error(f'Error getting server time: {response.status}')
                        return None
        except Exception as e:
            logger.error(f'Error fetching server time: {str(e)}')
            return None

    async def sync_time(self):
        server_time = await self.get_server_time()
        if server_time:
            local_time = int(time.time() * 1000)
            self.time_offset = server_time - local_time
            logger.info(f'Time offset: {self.time_offset}ms')
            return True
        return False

    def _get_timestamp(self):
        return int(time.time() * 1000) + self.time_offset

    def _generate_signature(self, params):
        query_string = urllib.parse.urlencode(params)
        return hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def get_listen_key(self):
        try:
            async with aiohttp.ClientSession() as session:
                url = f'{self.base_url}/userDataStream'
                header = {'X-MBX-APIKEY': self.api_key}

                async with session.post(url, headers=header) as response:
                    if response.status != 200:
                        logger.error(f'❌ Failed to get listen key: {response.status}')
                        return None

                    data = await response.json()
                    self.listen_key = data.get('listenKey')
                    logger.info(f'✅ Listen key obtained: {self.listen_key}')

                    return self.listen_key
        except Exception as e:
            logger.error(f'Error getting listen key: {str(e)}')
            return None

    async def keepalive_listen_key(self):
        while self.is_connected:
            await asyncio.sleep(1800)
            if not self.is_connected:
                break
            try:
                async with aiohttp.ClientSession() as session:
                    url = f'{self.base_url}/userDataStream'
                    headers = {'X-MBX-APIKEY': self.api_key}
                    params = {'listenKey': self.listen_key}

                    async with session.put(url, headers=headers, params=params) as response:
                        if response.status != 200:
                            logger.error('❌ Failed to renew listen key')
                        else:
                            logger.info('✅ Listen key renewed')
            except Exception as e:
                logger.error(f'Error renewing listen key: {str(e)}')

    async def get_user_balance(self):
        try:
            if not await self.sync_time():
                logger.error('Failed to sync time with Binance server')
                return

            async with aiohttp.ClientSession() as session:
                params = {
                    'timestamp': self._get_timestamp(),
                    'recvWindow': 5000
                }

                params['signature'] = self._generate_signature(params)

                url = f'{self.base_url}/account'
                headers = {'X-MBX-APIKEY': self.api_key}

                async with session.get(url, headers=headers, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f'Error getting user balance: {error_text}')
                        return

                    data = await response.json()

                    if 'balances' not in data:
                        logger.info(f'No balance data received')

                    logger.info(f'✅ User balance retrieved successfully')

                    self.user_balance = {
                        balance['asset']: {
                            'free': balance['free'],
                            'locked': balance['locked']
                        }
                        for balance in data['balances']
                        if float(balance['free']) > 0 or float(balance['locked']) > 0
                    }
                    logger.info(f'User balance processed: {len(self.user_balance)} assets:')
        except Exception as e:
            logger.error(f'Error fetching user balance: {str(e)}')

    async def get_symbols_from_trades(self):
        symbols = []

        for trade in self.trades:
            symbol = trade.get('symbol').lower()
            base_asset = symbol.replace('usdt', '')

            balance = self.user_balance.get(base_asset.upper())

            if not balance:
                logger.warning(f'❌ No balance for {base_asset}, skipping...')
                continue

            if float(balance.get('free')) >= float(trade.get('quantity')):
                symbols.append(f'{trade.get("symbol").lower()}@trade')
        return symbols

    async def subscribe_to_market_data_stream(self, streams):
        """Подписка на market data"""
        try:
            stream_url = f'{self.ws_stream_url}?streams={streams}'

            # Закрываем старое соединение если есть
            if self.market_connection:
                try:
                    await self.market_connection.close()
                except:
                    pass

            # Создаем новое соединение
            self.market_connection = await websockets.connect(stream_url)
            logger.info(f'✅ Connected to market data stream: {streams}')

            # Запускаем только одного слушателя
            # Проверяем, нет ли уже активной задачи
            market_tasks = [t for t in self._tasks if t.get_name() == f'market_listener_{self.user_id}']
            for task in market_tasks:
                if not task.done():
                    task.cancel()

            # Создаем новую задачу с именем
            market_task = asyncio.create_task(
                self.listen_market_data(),
                name=f'market_listener_{self.user_id}'
            )
            self._tasks.append(market_task)
        except Exception as e:
            logger.error(f'Error subscribing to market data stream: {str(e)}')

    async def listen_user_data(self):
        try:
            logger.info(f'🎧 Starting to listen to User Data Stream...')
            while self.is_connected and self.connection:
                try:
                    message = await asyncio.wait_for(self.connection.recv(), timeout=1.0)
                    await self.handle_user_message(message)
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosedError as e:
                    logger.error(f'❌ User Data Stream connection closed: {e}')
                    break
                except Exception as e:
                    logger.error(f'❌ Error in user data listener: {e}')
                    break
            logger.info(f'User data listener stopped for user {self.user_id}')
        except Exception as e:
            logger.error(f'❌ Error in user data listener: {e}')
            self.is_connected = False

    async def listen_market_data(self):
        """Упрощенный слушатель market data без параллельного recv"""
        try:
            logger.info(f'🎧 Starting market data listener for user {self.user_id}')

            try:
                async for message in self.market_connection:
                    if not self.is_connected:
                        break
                    await self.handle_market_message(message)
            except websockets.exceptions.ConnectionClosedError:
                logger.info(f'Market connection closed for user {self.user_id}')
            except Exception as e:
                logger.error(f'Error in market data async for: {e}')
        except Exception as e:
            logger.error(f'❌ Error in market data listening: {str(e)}')
            self.is_connected = False

    async def handle_market_message(self, message):
        try:
            data = json.loads(message)

            if 'stream' in data:
                stream_data = data['data']
                if stream_data.get('e') == 'trade':
                    await self.check_profit(stream_data)
            else:
                if data.get('e') == 'trade':
                    await self.check_profit(data)
        except Exception as e:
            logger.error(f'Error handling market message: {str(e)}')

    async def handle_user_message(self, message):
        try:
            data = json.loads(message)
            event_type = data.get('e')

            logger.info(f'User event: {event_type} - {data}')

            if event_type == 'executionReport':
                await self.handle_execution_report(data)

        except Exception as e:
            logger.error(f'Error handling user message: {str(e)}')

    async def handle_execution_report(self, data):
        order_status = data.get('X')
        order_type = data.get('o')
        side = data.get('S')
        symbol = data.get('s')

        logger.info(f'📊 Order Update: {side} {symbol} - Status: {order_status}')

        # Обрабатываем разные статусы ордеров
        if order_status in ['EXPIRED', 'CANCELED', 'REJECTED']:
            logger.warning(f"⚠️ Order {order_status}: {symbol} {side}")

            # Если это продажа и ордер истек, уведомляем торговый сервис
            if side == 'SELL' and order_status == 'EXPIRED':
                await self._handle_expired_sell_order(symbol, data)

        # Логируем событие
        elif side == 'BUY' and order_status == 'FILLED' and order_type == 'MARKET':
            quantity = float(data.get('l'))
            price = float(data.get('L'))

            logger.info(f'🛒 BUY Order FILLED: {symbol} {quantity} @ {price}')

            # Поиск сделки по order_id
            order_id = data.get('i')
            trade_exists = False

            if hasattr(self, 'trades'):
                for trade in self.trades:
                    if trade.get('buy_order_id') == order_id:
                        trade_exists = True
                        logger.info(f"ℹ️ Trade already exists for order {order_id}")
                        break

            if not trade_exists:
                logger.info(f"⚠️ No existing trade found for order {order_id}")

    async def _handle_expired_sell_order(self, symbol: str, order_data: dict):
        """Обработка истекших ордеров на продажу"""
        try:
            order_id = order_data.get('i')
            quantity = float(order_data.get('q', 0))

            logger.info(f"🔄 Handling expired SELL order {order_id} for {symbol}")

            # Уведомляем торговый сервис об истекшем ордере
            if hasattr(self, 'trading_service') and self.trading_service:
                # Получаем текущую рыночную цену
                current_price = await self.trading_service._get_current_price(symbol)

                # Ищем соответствующую сделку
                if hasattr(self, 'trades'):
                    for trade in self.trades:
                        if trade.get('symbol', '').lower() == symbol.lower():
                            trade_id = trade.get('trade_id')
                            if trade_id:
                                try:
                                    db_trade = await Trade.objects.aget(id=trade_id)
                                    logger.info(f"⚡ Retrying close for trade {trade_id}")
                                    # Торговый сервис сам попробует IOC ордер
                                    break
                                except Trade.DoesNotExist:
                                    continue

        except Exception as e:
            logger.error(f"❌ Error handling expired order: {e}")

    async def handle_new_purchase(self, symbol: str, quantity: float, price: float, trade_data: dict):
        logger.info(f'🛒 NEW PURCHASE DETECTED: {symbol} {quantity} @ {price}')

        try:
            exchange_name = getattr(self, 'exchange_name', 'binance')

            exchange = await Exchange.objects.aget(name__iexact=exchange_name)

            # 1. Создаем запись о сделке в БД
            trade_record = await Trade.objects.acreate(
                user_id=self.user_id,
                exchange=exchange,
                symbol=symbol.upper(),
                quantity=quantity,
                buy_price=price,
                buy_order_id=trade_data.get('i'),
                status='active',
                target_profit_percent=self.trading_service.config['target_profit_percent'],
                stop_loss_percent=self.trading_service.config['stop_loss_percent'],
                commission_paid=0.0
            )

            logger.info(f"📝 Created trade record in DB with ID: {trade_record.id}")

            # 2. Создаем объект сделки для активного списка с ID из БД
            new_trade = {
                'symbol': symbol.lower(),
                'quantity': quantity,
                'avg_buy_price': price,
                'trade_id': trade_record.id,
                'buy_order_id': trade_data.get('i'),
                'closed': False,
                'created_at': time.time()
            }

            logger.info(f"📋 Trade object created:")
            logger.info(f"   Symbol: {new_trade['symbol']}")
            logger.info(f"   Quantity: {new_trade['quantity']}")
            logger.info(f"   Buy price: ${new_trade['avg_buy_price']}")
            logger.info(f"   Trade ID: {new_trade['trade_id']}")
            logger.info(f"   Order ID: {new_trade['buy_order_id']}")

            # 3. Добавляем в список активных сделок
            self.trades.append(new_trade)
            logger.info(f"✅ Trade added. Total active trades: {len(self.trades)}")

            # 4. Подписываемся на обновления цены
            await self.subscribe_to_market_data_stream(f'{symbol.lower()}@trade')

        except Exception as e:
            logger.error(f"❌ Error creating trade record: {e}")
            fallback_trade = {
                'symbol': symbol.lower(),
                'quantity': quantity,
                'avg_buy_price': price,
                'buy_order_id': trade_data.get('i'),
                'closed': False,
                'created_at': time.time(),
                'has_no_id': True  # Помечаем, что нет ID
            }
            self.trades.append(fallback_trade)
            logger.warning(f"⚠️ Added trade without DB ID. Will try to recover later.")

    async def check_profit(self, market_data):
        try:
            symbol = market_data.get('s', '').lower()
            market_price = float(market_data.get('p', 0))

            if not symbol or market_price <= 0:
                logger.debug(f'⚠️ Invalid market data for {symbol}: price={market_price}')
                return

            # Находим все активные сделки для этого символа
            active_trades = [
                trade for trade in self.trades
                if trade.get('symbol', '').lower() == symbol
            ]

            if not active_trades:
                logger.debug(f'ℹ️ No active trades found for {symbol}')
                return

            # Для каждой сделки логируем анализ
            for trade in active_trades:
                buy_price = float(trade.get('avg_buy_price', 0))

                if buy_price > 0:
                    profit_percent = ((market_price - buy_price) / buy_price) * 100

                    logger.info(f'''
                        📊 Profit Analysis for {symbol.upper()}
                        ├── Buy Price: ${buy_price:.6f}
                        ├── Current Price: ${market_price:.6f}
                        ├── Profit/Loss: {profit_percent:+.2f}%
                        └── Status: {'🟢 PROFIT' if profit_percent > 0 else '🔴 LOSS'}
                    ''')

            # Вызываем торговый сервис только один раз для символа
            if self.trading_service:
                try:
                    logger.info(f"🔄 Calling trading service for {symbol}...")
                    trade_closed = await self.trading_service.check_and_close_trades(market_data)

                    if trade_closed:
                        logger.info(f"✅ Trade for {symbol} was closed by trading service")
                    else:
                        logger.info(f"ℹ️ Trading service did not close trade for {symbol}")
                except Exception as e:
                    logger.error(f"❌ Trading service error: {e}")

        except Exception as e:
            logger.error(f'❌ Error in profit calculation: {str(e)}', exc_info=True)

    async def _log_profit_analysis(self, symbol, market_price, trade_data=None):
        """Логирование анализа прибыли - безопасная версия"""
        try:
            # Проверяем, что trade_data не None
            if trade_data is None:
                # Пытаемся найти сделку
                found_trades = [t for t in self.trades if t.get('symbol', '').lower() == symbol]

                if not found_trades:
                    logger.info(f"ℹ️ No trade data available for {symbol} (might be closed)")
                    return

                trade_data = found_trades[0]

            # Проверяем наличие необходимых полей
            if not isinstance(trade_data, dict):
                logger.error(f"❌ Invalid trade_data type: {type(trade_data)}")
                return

            avg_buy_price = float(trade_data.get('avg_buy_price'))

            if avg_buy_price is None:
                logger.warning(f"⚠️ No buy price in trade data for {symbol}")
                return

            try:
                buy_price = float(avg_buy_price)
                if buy_price <= 0:
                    logger.warning(f"⚠️ Invalid buy price for {symbol}: {buy_price}")
                    return

                profit_percent = ((market_price - buy_price) / buy_price) * 100

                logger.info(f'''
                    📊 Profit Analysis for {symbol.upper()}
                    ├── Buy Price: ${buy_price:.6f}
                    ├── Current Price: ${market_price:.6f}
                    ├── Profit/Loss: {profit_percent:+.2f}%
                    └── Status: {'🟢 PROFIT' if profit_percent > 0 else '🔴 LOSS'}
                ''')

            except (ValueError, TypeError) as e:
                logger.error(f"❌ Error converting buy price for {symbol}: {e}")

        except Exception as e:
            logger.error(f'❌ Error in profit analysis logging: {str(e)}')

    async def subscribe_to_symbol(self, symbol: str):
        """Подписка на обновления цены для конкретного символа"""
        try:
            symbol_lower = symbol.lower()
            stream_name = f"{symbol_lower}@trade"

            # Проверяем, подписаны ли уже
            if stream_name in self.active_streams:
                logger.debug(f"ℹ️ Already subscribed to {symbol_lower}")
                return True

            # Добавляем в активные стримы
            self.active_streams.add(stream_name)

            # Переподключаемся с новым списком стримов
            await self._reconnect_market_streams()

            logger.info(f"✅ Subscribed to {symbol_lower}")
            return True

        except Exception as e:
            logger.error(f"❌ Error subscribing to {symbol}: {e}")
            return False

    async def unsubscribe_from_symbol(self, symbol: str):
        """Отписка от стрима для символа"""
        try:
            stream_name = f"{symbol.lower()}@trade"

            if stream_name not in self.active_streams:
                logger.info(f"ℹ️ Stream {stream_name} not in active subscriptions")
                return False

            # Удаляем из активных стримов
            self.active_streams.remove(stream_name)
            logger.info(f"📭 Removed {stream_name} from active streams")

            if not self.active_streams:
                # Если больше нет стримов, закрываем соединение
                if self.market_connection:
                    await self.market_connection.close()
                    self.market_connection = None
                    logger.info("🔌 Closed market connection (no active streams)")
                return True

            # Переподключаемся с обновленным списком стримов
            await self._reconnect_market_streams()
            return True

        except Exception as e:
            logger.error(f"❌ Error unsubscribing from {symbol}: {e}")
            return False

    async def _reconnect_market_streams(self):
        """Переподключение к market data с обновленным списком стримов"""
        try:
            if not self.active_streams:
                logger.info("ℹ️ No active streams to reconnect to")
                return

            # Создаем новый URL со всеми активными стримами
            streams_list = sorted(self.active_streams)
            streams = '/'.join(streams_list)
            stream_url = f'{self.ws_stream_url}?streams={streams}'

            # Закрываем старое соединение если есть
            if self.market_connection:
                try:
                    await self.market_connection.close()
                except:
                    pass

            # Создаем новое соединение
            self.market_connection = await websockets.connect(stream_url)
            logger.info(f"🔄 Reconnected to {len(streams_list)} market streams")
            logger.info(f"   Active streams: {streams_list}")

            # Перезапускаем слушателя
            await self._restart_market_listener()

        except Exception as e:
            logger.error(f"❌ Error reconnecting market streams: {e}")

    async def _restart_market_listener(self):
        """Перезапуск слушателя market data"""
        # Отменяем старую задачу
        market_tasks = [t for t in self._tasks if t.get_name() == f'market_listener_{self.user_id}']
        for task in market_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Создаем новую задачу
        if self.market_connection:
            market_task = asyncio.create_task(
                self.listen_market_data(),
                name=f'market_listener_{self.user_id}'
            )
            self._tasks.append(market_task)
            logger.info("🔄 Restarted market data listener")

    async def subscribe_to_market_data_stream(self, streams):
        """Подписка на market data с отслеживанием активных стримов"""
        try:
            # Если streams - строка, преобразуем в список
            if isinstance(streams, str):
                streams_list = [streams]
            else:
                streams_list = streams

            # Добавляем в активные стримы
            for stream in streams_list:
                self.active_streams.add(stream)

            stream_url = f'{self.ws_stream_url}?streams={'/'.join(sorted(self.active_streams))}'

            # Закрываем старое соединение если есть
            if self.market_connection:
                try:
                    await self.market_connection.close()
                except:
                    pass

            # Создаем новое соединение
            self.market_connection = await websockets.connect(stream_url)
            logger.info(f'✅ Connected to market data streams: {len(self.active_streams)} active')
            logger.info(f'   Active streams: {sorted(self.active_streams)}')

            # Запускаем слушателя
            await self._restart_market_listener()

        except Exception as e:
            logger.error(f'Error subscribing to market data stream: {str(e)}')

    async def _preload_symbol_info(self):
        """Предзагрузка информации о символах"""
        try:
            async with aiohttp.ClientSession() as session:
                url = f'{self.base_url}/exchangeInfo'
                async with session.get(url, timeout=2) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.symbol_info_cache = {
                            symbol['symbol']: symbol
                            for symbol in data.get('symbols', [])
                        }
                        logger.info(f'✅ Preloaded info for {len(self.symbol_info_cache)} symbols')
                    else:
                        logger.warning(f'⚠️ Failed to preload symbol info: {response.status}')
        except asyncio.TimeoutError:
            logger.warning('⚠️ Timeout preloading symbol info')
        except Exception as e:
            logger.warning(f'⚠️ Could not preload symbol info: {e}')

    async def _start_optimized_listeners(self):
        """Запуск оптимизированных слушателей"""
        try:
            # Создаем и сохраняем задачи
            user_task = asyncio.create_task(self.listen_user_data())
            keepalive_task = asyncio.create_task(self.keepalive_listen_key())
            self._tasks.extend([user_task, keepalive_task])

            symbols = await self.get_symbols_from_trades()
            if symbols:
                streams = '/'.join(symbols)
                await self.subscribe_to_market_data_stream(streams)

            logger.info(f"✅ All listeners started for user {self.user_id}")
            logger.info(f"   Active tasks: {len(self._tasks)}")

        except Exception as e:
            logger.error(f"❌ Error starting listeners: {e}")
            raise

    async def connect(self):
        """Оптимизированное соединение с минимальными задержками"""
        start_time = time.time()
        try:
            # 1. Приоритетное получение listen_key
            listen_key = await asyncio.wait_for(
                self.get_listen_key(),
                timeout=3.0
            )
            if not listen_key:
                raise Exception('Failed to get listen_key')

            # 2. Сначала получаем listen_key и синхронизируем время
            logger.info(f'Getting listen key and syncing time...')
            listen_key_task = asyncio.create_task(self.get_listen_key())
            sync_time_task = asyncio.create_task(self.sync_time())

            listen_key, _ = await asyncio.gather(listen_key_task, sync_time_task)

            if not listen_key:
                raise Exception('Failed to get listen_key')

            # 3. Подключаемся к WebSocket
            logger.info(f'Connecting to Binance WebSocket...')
            ws_url = f'{self.ws_url}/{listen_key}'

            try:
                self.connection = await websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=1,
                    max_size=2 ** 20,
                )
            except Exception as e:
                logger.error(f'❌ WebSocket connection error: {e}')
                raise

            self.is_connected = True
            logger.info(f'✅ WebSocket connected in {time.time() - start_time:.2f}s')

            # 4. Параллельно получаем баланс и информацию о символах
            logger.info(f'Fetching user balance and symbol info...')
            balance_task = asyncio.create_task(self.get_user_balance())
            preload_task = asyncio.create_task(self._preload_symbol_info())

            try:
                await asyncio.gather(balance_task, preload_task, return_exceptions=True)
            except Exception as e:
                logger.warning(f'⚠️ Some tasks failed: {e}')

            # 5. Инициализируем торговый сервис
            from .trading import TradingService
            self.trading_service = TradingService(self)

            # 6. Запускаем слушателей
            await self._start_optimized_listeners()

            return True

        except asyncio.TimeoutError:
            logger.error('❌ Connection timeout')
            return False
        except Exception as e:
            logger.error(f'❌ Connection error: {e}', exc_info=True)
            raise

    async def disconnect(self):
        """Полное отключение от всех соединений и отмена задач"""
        try:
            logger.info(f'Starting disconnect for user {self.user_id}...')
            self.is_connected = False

            # Даем время задачам увидеть флаг is_connected = False
            await asyncio.sleep(0.2)

            # Отменяем все задачи
            for task in self._tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=0.5)
                    except (asyncio.CancelledError, asyncio.TimeoutError):
                        pass

            # Закрываем соединения
            close_tasks = []

            if self.connection:
                close_tasks.append(self.connection.close())

            if self.market_connection:
                close_tasks.append(self.market_connection.close())

            if close_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*close_tasks), timeout=2.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass

            # Очищаем список задач
            self._tasks.clear()

            logger.info(f"✅ Full disconnect completed for user {self.user_id}")

        except Exception as e:
            logger.error(f"Error during disconnect: {str(e)}")

    async def stop(self):
        """Альтернативный метод для полной остановки"""
        await self.disconnect()