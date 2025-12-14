import asyncio
import websockets
import logging
import aiohttp
import time
import hmac
import hashlib
import urllib.parse
import json

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
        self._tasks = []  # Для хранения фоновых задач

        if testnet:
            self.base_url = 'https://testnet.binance.vision/api/v3'
            self.ws_url = 'wss://stream.testnet.binance.vision/ws'
            self.ws_stream_url = 'wss://stream.testnet.binance.vision/stream'
        else:
            self.base_url = 'https://api.binance.com/api/v3'
            self.ws_url = 'wss://stream.binance.com:9443/ws'
            self.ws_stream_url = 'wss://stream.binance.com:9443/stream'

        self.connection = None
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
            await asyncio.sleep(1800)  # 30 минут
            if not self.is_connected:  # Проверка перед обновлением
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
        """
        :param streams: <asset>@trade or [<asset>@trade, <asset>@trade, ...]
        :return:
        """
        try:
            stream_url = f'{self.ws_stream_url}?streams={streams}'

            self.market_connection = await websockets.connect(stream_url)
            logger.info(f'✅ Connected to market data stream: {streams}')

            # Создаем и сохраняем задачу
            market_task = asyncio.create_task(self.listen_market_data())
            self._tasks.append(market_task)
        except Exception as e:
            logger.error(f'Error subscribing to market data stream: {str(e)}')

    async def listen_user_data(self):
        try:
            logger.info(f'🎧 Starting to listen to User Data Stream...')
            while self.is_connected and self.connection:
                try:
                    # Используем recv() с таймаутом вместо async for
                    message = await asyncio.wait_for(self.connection.recv(), timeout=1.0)
                    await self.handle_user_message(message)
                except asyncio.TimeoutError:
                    continue  # Просто продолжаем слушать
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
        try:
            while self.is_connected and self.market_connection:
                try:
                    # Используем recv() с таймаутом вместо async for
                    message = await asyncio.wait_for(self.market_connection.recv(), timeout=1.0)
                    await self.handle_market_message(message)
                except asyncio.TimeoutError:
                    continue
                except websockets.exceptions.ConnectionClosedError as e:
                    logger.error(f'❌ Market Data Stream connection closed: {e}')
                    break
                except Exception as e:
                    logger.error(f'❌ Error in market data listening: {str(e)}')
                    break
            logger.info(f'Market data listener stopped for user {self.user_id}')
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
            logger.error(f'Error listening to market data stream: {str(e)}')

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

        if side == 'BUY' and order_status == 'FILLED' and order_type == 'MARKET':
            quantity = float(data.get('l'))
            price = float(data.get('L'))

            if quantity > 0 and price > 0:
                await self.handle_new_purchase(symbol, quantity, price, data)

    async def handle_new_purchase(self, symbol: str, quantity: float, price: float, trade_data: dict):
        logger.info(f'🛒 NEW PURCHASE DETECTED: {symbol} {quantity} @ {price}')

        self.trades.append({
            'symbol': symbol.lower(),
            'quantity': quantity,
            'avg_buy_price': price,
            'order_id': trade_data.get('i')
        })

        await self.subscribe_to_market_data_stream(f'{symbol.lower()}@trade')

    async def check_profit(self, market_data):
        try:
            symbol = market_data.get('s').lower()
            market_price = float(market_data.get('p'))
            market_quantity = float(market_data.get('q'))

            if not symbol or market_price <= 0:
                logger.warning(f'⚠️ Invalid market data for {symbol}: price={market_price}')
                return

            trade_data = next((trade for trade in self.trades
                               if trade.get('symbol', '').lower() == symbol), None)

            if not trade_data:
                logger.warning(f'⚠️ Have not data for {symbol} in ')

            avg_buy_price = float(trade_data.get('avg_buy_price'))
            balance_quantity = float(trade_data.get('quantity'))

            sell_price_after_commission = market_price * self.commission
            price_difference = sell_price_after_commission - avg_buy_price

            is_profit = sell_price_after_commission > avg_buy_price and market_quantity >= balance_quantity

            logger.info(f'''
            📊 Profit Analysis for {symbol.upper()}
            ├── Buy Price: ${avg_buy_price:.6f}
            ├── Current Price: ${market_price:.6f}
            ├── After Commission (1%): ${sell_price_after_commission:.6f}
            ├── Price defference: ${price_difference:+.6f}
            └── Status: {'🟢 PROFIT' if is_profit else '🔴 LOSS'}
            ''')
        except Exception as e:
            logger.error(f'❌ Error in profit calculation: {str(e)}')

    async def connect(self):
        try:
            listen_key = await self.get_listen_key()
            if not listen_key:
                raise Exception('Failed to get listen_key')

            logger.info('Connecting to Binance Websocket...')
            self.connection = await websockets.connect(
                f'{self.ws_url}/{listen_key}',
                ping_interval=60,
                ping_timeout=30
            )
            self.is_connected = True
            logger.info(f'✅ Successfully connected to Binance Websocket for user {self.user_id}')

            logger.info(f'Getting user balance for user {self.user_id}...')
            await self.get_user_balance()

            # Создаем и сохраняем задачи
            user_task = asyncio.create_task(self.listen_user_data())
            keepalive_task = asyncio.create_task(self.keepalive_listen_key())
            self._tasks.extend([user_task, keepalive_task])

            symbols = await self.get_symbols_from_trades()
            if symbols:
                streams = '/'.join(symbols)
                await self.subscribe_to_market_data_stream(streams)

            return True
        except Exception as e:
            logger.error(f'❌ Connection error: {e}')
            if self.connection:
                await self.connection.close()
            raise e

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