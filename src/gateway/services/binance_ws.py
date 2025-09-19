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
    def __init__(self, user_id: int, api_key: str, secret_key: str, testnet: bool = True):
        self.user_id = user_id
        self.api_key = api_key
        self.secret_key = secret_key
        self.time_offset = 0

        if testnet:
            self.base_url = 'https://testnet.binance.vision/api/v3'
            self.ws_url = 'wss://stream.testnet.binance.vision/ws'
            self.ws_stream_url = 'wss://stream.testnet.binance.vision/stream'
        else:
            self.base_url = 'https://api.binance.com/api/v3'
            self.ws_url = 'wss://stream.binance.com:9443/ws'
            self.ws_stream_url = 'wss://stream.binance.com:9443/stream'

        self.connection = None
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
                    logger.info(f'User balance response received')
                    logger.debug(f'User balance response: {data}')

                    if 'balances' in data:
                        self.user_balance = {
                            balance['asset']: {
                                'free': balance['free'],
                                'locked': balance['locked']
                            }
                            for balance in data['balances']
                            if float(balance['free']) > 0 or float(balance['locked']) > 0
                        }
                        logger.info(f'User balance processed: {len(self.user_balance)} assets')

        except Exception as e:
            logger.error(f'Error fetching user balance: {str(e)}')

    async def test_websocket_connection(self):
        try:
            async with websockets.connect(self.ws_url, ping_interval=10, ping_timeout=5) as test_ws:
                logger.info("✅ WebSocket endpoint is reachable")
                return True
        except Exception as e:
            logger.error(f"❌ WebSocket endpoint not reachable: {e}")
            return False


    async def connect(self):
        try:
            if not await self.test_websocket_connection():
                raise Exception('❌WebSocket endpoint not reachable')

            logger.info(f'Getting user balance for user {self.user_id}...')
            await self.get_user_balance()

            if self.user_balance:
                logger.info(f'User balance retrieved successfully')
            else:
                logger.warning(f'No balance data received')

            logger.info('Connecting to Binance Websocket...')

            self.connection = await websockets.connect(self.ws_url, ping_interval=60, ping_timeout=30)
            self.is_connected = True

            logger.info(f'✅ Successfully connected to Binance Websocket for user {self.user_id}')

            streams = []

            for asset in self.user_balance.keys():
                if asset == 'USDT':
                    continue
                streams.append(f'{asset.lower()}usdt@trade')

            subscribe_message = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": self.user_id
            }

            await self.connection.send(json.dumps(subscribe_message))
            logger.info(f'Subscribed to {len(streams)} trade streams: {streams}')

            asyncio.create_task(self.listen())

            return True

        except Exception as e:
            logger.error(f'❌ Connection error: {e}')
            if self.connection:
                await self.connection.close()
            raise e

    async def listen(self):
        try:
            async for message in self.connection:
                try:
                    data = json.loads(message)
                    logger.info(f"📨 Received data: {data}")
                except json.JSONDecodeError:
                    logger.warning(f"Received non-JSON message: {message}")

        except websockets.exceptions.ConnectionClosed as e:
            logger.info(f"Connection closed for user {self.user_id}: {e}")
            self.is_connected = False
        except Exception as e:
            logger.error(f'❌ Listening error: {e}')
            self.is_connected = False

    async def disconnect(self):
        if self.is_connected and self.connection:
            self.is_connected = False
            await self.connection.close()
            logger.info(f"Connection closed for user {self.user_id}")