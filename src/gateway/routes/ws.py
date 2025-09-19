from fastapi import APIRouter, HTTPException
from ..services.binance_ws import BinanceWebsocket

router = APIRouter()
active_connections = {}

@router.post('/users/{user_id}/exchanges/binance/ws/start/')
async def start_websocket(user_id: int, api_keys: dict):
    try:
        if user_id in active_connections:
            await active_connections[user_id].disconnect()

        ws_binance = BinanceWebsocket(
            user_id=user_id,
            api_key=api_keys.get('api_key'),
            secret_key=api_keys.get('secret_key'),
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
        raise HTTPException(status_code=500, detail=f'Error starting websocket: {str(e)}')

@router.post('/users/{user_id}/exchanges/binance/ws/stop/')
async def stop_websocket(user_id: int):
    try:
        if user_id in active_connections:
            await active_connections[user_id].disconnect()
            del active_connections[user_id]

            return {
                'status': 'Success',
                'message': f'WebSocket connection to Binance stopped for user {user_id}',
            }
        else:
            raise HTTPException(status_code=404, detail='No active connection found')

    except Exception as e:
        raise HTTPException(status_code=500, detail=f'Error stopping websocket: {str(e)}')
