import asyncio

from fastapi import APIRouter, HTTPException, Body
from ..services.binance_ws import BinanceWebsocket
import logging

logger = logging.getLogger(__name__)

router = APIRouter()
active_connections = {}


@router.post('/users/{user_id}/exchanges/binance/ws/start/')
async def start_websocket(user_id: int, api_keys: dict = Body(), trades: list = Body()):
    try:
        logger.info(f"=== STARTING WEBSOCKET FOR USER {user_id} ===")
        logger.info(f"API keys received: {bool(api_keys)}")
        logger.info(f"Trades received: {len(trades) if trades else 0}")

        # Если уже есть соединение, останавливаем его
        if user_id in active_connections:
            logger.info(f"Found existing connection for user {user_id}, stopping it...")
            try:
                await active_connections[user_id].disconnect()
                del active_connections[user_id]
                await asyncio.sleep(0.5)  # Даем время на полную остановку
            except Exception as e:
                logger.warning(f"Error stopping existing connection: {e}")

        ws_binance = BinanceWebsocket(
            user_id=user_id,
            api_key=api_keys.get('api_key'),
            secret_key=api_keys.get('secret_key'),
            trades=trades,
            testnet=True
        )
        await ws_binance.connect()

        active_connections[user_id] = ws_binance

        return {
            'status': 'Success',
            'exchange': 'binance',
            'username': user_id,
            'message': f'WebSocket connection to Binance started for user {user_id}',
        }
    except Exception as e:
        logger.error(f"Error starting websocket: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error starting websocket: {str(e)}')


@router.post('/users/{user_id}/exchanges/binance/ws/stop/')
async def stop_websocket(user_id: int):
    try:
        logger.info(f"=== STOPPING WEBSOCKET FOR USER {user_id} ===")

        if user_id in active_connections:
            ws_instance = active_connections[user_id]

            # Пытаемся использовать метод stop() если он есть, иначе disconnect()
            if hasattr(ws_instance, 'stop'):
                await ws_instance.stop()
            else:
                await ws_instance.disconnect()

            # Удаляем из активных соединений
            del active_connections[user_id]

            # Даем время на полную остановку
            await asyncio.sleep(0.3)

            logger.info(f"Successfully stopped WebSocket for user {user_id}")

            return {
                'status': 'Success',
                'message': f'WebSocket connection to Binance stopped for user {user_id}',
                'details': 'All connections and tasks terminated'
            }
        else:
            logger.warning(f"No active connection found for user {user_id}")
            raise HTTPException(status_code=404, detail='No active connection found')

    except Exception as e:
        logger.error(f"Error stopping websocket: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f'Error stopping websocket: {str(e)}')


@router.get('/users/{user_id}/exchanges/binance/status/')
async def get_connection_status(user_id: int):
    """Проверяет состояние соединения"""
    if user_id in active_connections:
        ws_instance = active_connections[user_id]
        return {
            'status': 'active',
            'user_id': user_id,
            'is_connected': ws_instance.is_connected,
            'has_user_connection': ws_instance.connection is not None,
            'has_market_connection': ws_instance.market_connection is not None,
            'active_tasks': len(getattr(ws_instance, '_tasks', [])),
            'connection_time': getattr(ws_instance, '_connect_time', 'unknown')
        }
    else:
        return {'status': 'inactive', 'user_id': user_id}


@router.get('/active-connections/')
async def get_all_active_connections():
    """Показывает все активные соединения"""
    connections_info = {}

    for user_id, ws_instance in active_connections.items():
        connections_info[user_id] = {
            'is_connected': ws_instance.is_connected,
            'has_user_connection': ws_instance.connection is not None,
            'has_market_connection': getattr(ws_instance, 'market_connection', None) is not None,
            'tasks_count': len(getattr(ws_instance, '_tasks', [])),
            'active_tasks': [t.get_name() for t in getattr(ws_instance, '_tasks', []) if not t.done()]
        }

    return {
        'total_connections': len(active_connections),
        'connections': connections_info
    }