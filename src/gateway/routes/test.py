from fastapi import APIRouter, HTTPException
import json, time

router = APIRouter()

@router.post('/test/{user_id}/simulate-buy')
async def simulate_buy_order(user_id: int):
    try:
        mock_order = {
            "e": "executionReport",
            "E": int(time.time() * 1000),
            "s": "ETHUSDT",
            "c": f"mockOrder_{user_id}",
            "S": "BUY",
            "o": "MARKET",
            "f": "GTC",
            "q": "0.00100000",
            "p": "0.00000000",
            "X": "FILLED",
            "i": 999000 + user_id,
            "l": "0.00100000",
            "z": "0.00100000",
            "L": "50000.00000000",
            "n": "0.00000000",
            "N": None,
            "T": int(time.time() * 1000),
            "t": 999000 + user_id,
            "w": True,
            "m": False,
            "M": True
        }

        from .ws import active_connections

        if user_id in active_connections:
            ws_instance = active_connections[user_id]
            mock_message = json.dumps(mock_order)
            await ws_instance.handle_user_message(mock_message)

            return {
                'status': 'SUCCESS',
                'message': 'Mock buy order simulated',
                'order_data': mock_order,
            }
        else:
            raise HTTPException(status_code=404, detail='User WebSocket not found')

    except Exception as e:
        raise HTTPException(status_code=500, detail={str(e)})