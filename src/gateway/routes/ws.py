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

@router.post("/users/{user_id}/exchanges/binance/create_trade/")
async def create_trade_endpoint(user_id: int, trade_data: dict = Body(...)):
    """Создание быстрой сделки"""
    try:
        logger.info(f"⚡ Creating FAST trade for user {user_id}: {trade_data}")

        if user_id not in active_connections:
            raise HTTPException(status_code=400, detail="WebSocket connection not active")

        ws_instance = active_connections[user_id]

        if not hasattr(ws_instance, 'trading_service'):
            raise HTTPException(status_code=500, detail="Trading service not initialized")

        # Получаем данные
        symbol = trade_data.get('symbol', '').upper()
        usdt_amount = float(trade_data.get('usdt_amount', 0))
        order_type = trade_data.get('order_type', 'MARKET')
        create_trade_record = trade_data.get('create_trade_record', True)

        if not symbol:
            raise HTTPException(status_code=400, detail="Symbol is required")

        if usdt_amount <= 0:
            raise HTTPException(status_code=400, detail="Amount must be positive")

        # Используем расширенные возможности если они есть
        trading_service = ws_instance.trading_service

        # Проверяем, есть ли метод с контролем создания сделки
        if hasattr(trading_service, 'buy_with_usdt_optimized'):
            order_result = await trading_service.buy_with_usdt_optimized(
                symbol, usdt_amount, create_trade_record
            )
        elif order_type == 'LIMIT' and hasattr(trading_service, 'create_limit_buy_order'):
            # Используем лимитный ордер
            slippage = float(trade_data.get('slippage', 0.002))  # 0.2% по умолчанию
            order_result = await trading_service.create_limit_buy_order(
                symbol, usdt_amount, slippage
            )
        else:
            # Используем стандартный рыночный ордер
            order_result = await trading_service.buy_with_usdt(symbol, usdt_amount)

        if not order_result:
            raise HTTPException(status_code=400, detail="Failed to create trade")

        # Проверяем статус ордера
        order_status = order_result.get('status', 'UNKNOWN')
        is_filled = order_status == 'FILLED'

        return {
            "success": is_filled,
            "message": f"Trade {'executed' if is_filled else 'created'} for {symbol}",
            "order_id": order_result.get('orderId'),
            "order_status": order_status,
            "symbol": symbol,
            "amount_usdt": usdt_amount,
            "executed_qty": order_result.get('executedQty'),
            "cummulative_quote_qty": order_result.get('cummulativeQuoteQty'),
            "avg_price": trading_service._get_actual_sell_price(order_result) if is_filled else 0,
            "trade_created": create_trade_record,
            "active_trades_count": len(ws_instance.trades) if hasattr(ws_instance, 'trades') else 0
        }

    except ValueError as e:
        logger.error(f"Value error creating fast trade: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating fast trade: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))