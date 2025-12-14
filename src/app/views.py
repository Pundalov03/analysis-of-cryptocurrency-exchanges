import time

from django.contrib.auth import authenticate
from django.http import HttpResponse
from django.utils import timezone
import requests
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated, IsAdminUser
from rest_framework.response import Response
import logging

from .models import APIKey, Exchange, Trade
from .serializers import UserRegisterSerializer, UserLoginSerializer, ExchangeSerializer, APIKeySerializer, \
    TradeSerializer
from gateway.routes.ws import active_connections

logger = logging.getLogger(__name__)

HOST_FAST_API = 'http://localhost:8002'


# Create your views here.
def home(request):
    return HttpResponse('Hello World!')


@api_view(['POST'])
@permission_classes([AllowAny])
def register(request):
    serializer = UserRegisterSerializer(data=request.data)

    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    user = serializer.save()
    token, created = Token.objects.get_or_create(user=user)

    return Response({
        'token': token.key,
        'user_id': user.id,
        'email': user.email,
        'username': user.username,
        'role': user.profile.role,
    }, status=status.HTTP_201_CREATED)


@api_view(['POST'])
@permission_classes([AllowAny])
def login(request):
    serializer = UserLoginSerializer(data=request.data)

    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    user = authenticate(
        username=serializer.validated_data['username'],
        password=serializer.validated_data['password']
    )

    if user is not None:
        token, created = Token.objects.get_or_create(user=user)
        return Response({
            'token': token.key,
            'user_id': user.id,
            'email': user.email,
            'username': user.username,
            'role': user.profile.role,
        }, status=status.HTTP_200_OK)
    else:
        return Response({
            'error': 'Invalid credentials',
        }, status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
@permission_classes([AllowAny])
def logout(request):
    request.user.auth_token.delete()
    return Response({
        "message": "Successfully logged out",
    }, status=status.HTTP_200_OK)


@api_view(['POST'])
@permission_classes([IsAdminUser])
def add_exchange(request):
    serializer = ExchangeSerializer(data=request.data)

    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    exchange = serializer.save()

    return Response({
        'exchange_id': exchange.id,
        'exchange_name': exchange.name,
    })


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def add_api_key(request):
    serializer = APIKeySerializer(data=request.data, context={"request": request})

    if not serializer.is_valid():
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    api_key = serializer.save()

    return Response({
        'api_key': api_key.id,
    })


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def start_websocket(request, user_id):
    logger.info(f"=== Django: Starting WebSocket for user {user_id} ===")
    logger.info(f"Request user: {request.user.username} (id: {request.user.id})")

    try:
        # Проверяем авторизацию
        if request.user.id != user_id:
            logger.warning(f"Unauthorized: User {request.user.id} trying to access user {user_id}")
            return Response({
                'error': 'Unauthorized access',
            }, status=status.HTTP_403_FORBIDDEN)

        try:
            exchange = Exchange.objects.get(name='binance')
            logger.info(f"Exchange found: {exchange.name}")
        except Exchange.DoesNotExist:
            logger.error(f"Exchange 'binance' not found")
            return Response({
                'error': 'Exchange not found',
            }, status=status.HTTP_404_NOT_FOUND)

        # Получаем трейды
        trades_qs = Trade.objects.filter(
            user_id=user_id,
            exchange=exchange,
            status__in=['open', 'active']
        ).exclude(quantity=0)

        trades_list = []
        for trade in trades_qs:
            trades_list.append({
                'trade_id': trade.id,
                'symbol': trade.symbol.upper(),
                'quantity': float(trade.quantity),
                'avg_buy_price': float(trade.buy_price),
                'target_profit_percent': float(trade.target_profit_percent),  # ✅
                'stop_loss_percent': float(trade.stop_loss_percent),
            })

        logger.info(f"Found {len(trades_list)} active trades for user {user_id}")

        try:
            api_keys = APIKey.objects.get(
                user_id=user_id,
                exchange=exchange,
            )
            logger.info(f"API Key found for user {user_id}")
        except APIKey.DoesNotExist:
            logger.error(f"No API key found for user {user_id}")
            return Response({
                'error': 'API key for Binance not found. Please add API keys first using /add-api-key/ endpoint.',
            }, status=status.HTTP_404_NOT_FOUND)

        api_key = api_keys.api_key
        secret_key = api_keys.secret_key

        if not api_key or not secret_key:
            logger.error(f"API keys are empty for user {user_id}")
            return Response({
                'error': 'API key required'
            }, status=status.HTTP_400_BAD_REQUEST)

        payload = {
            'api_keys': {
                'api_key': api_key,
                'secret_key': secret_key,
            },
            'trades': trades_list,
        }

        logger.info(f"Sending request to FastAPI for user {user_id}")

        fastapi_url = f'{HOST_FAST_API}/users/{user_id}/exchanges/binance/ws/start/'

        try:
            response = requests.post(
                fastapi_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            logger.info(f"FastAPI response status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"FastAPI error: {response.status_code} - {response.text}")
                return Response({
                    'error': f'FastAPI error: {response.status_code}',
                    'details': response.text
                }, status=response.status_code)

            return Response(response.json(), status=response.status_code)

        except requests.exceptions.Timeout:
            logger.error(f"Timeout connecting to FastAPI")
            return Response({
                'error': 'FastAPI timeout',
                'message': 'FastAPI server did not respond in time',
            }, status=status.HTTP_504_GATEWAY_TIMEOUT)

    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to FastAPI server")
        return Response({
            'error': 'FastAPI server is not running',
            'message': 'Please, start the FastAPI server on port 8002',
        }, status=status.HTTP_503_SERVICE_UNAVAILABLE)

    except Exception as e:
        logger.error(f"Unexpected error in start_websocket: {str(e)}", exc_info=True)
        return Response({
            'error': 'Internal Server Error',
            'details': str(e),
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def stop_websocket(request, user_id):
    logger.info(f"=== Django: Stopping WebSocket for user {user_id} ===")

    try:
        fastapi_url = f'{HOST_FAST_API}/users/{user_id}/exchanges/binance/ws/stop/'

        try:
            response = requests.post(
                fastapi_url,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            logger.info(f"FastAPI stop response status: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"FastAPI stop error: {response.status_code} - {response.text}")
                return Response({
                    'error': f'FastAPI error: {response.status_code}',
                    'details': response.text
                }, status=response.status_code)

            return Response(response.json())

        except requests.exceptions.Timeout:
            logger.error(f"Timeout stopping FastAPI connection")
            return Response({
                'error': 'FastAPI timeout',
                'message': 'FastAPI server did not respond in time',
            }, status=status.HTTP_504_GATEWAY_TIMEOUT)

    except requests.exceptions.ConnectionError:
        logger.error(f"Cannot connect to FastAPI server")
        return Response({
            'error': 'FastAPI server is not running',
        }, status=status.HTTP_503_SERVICE_UNAVAILABLE)

    except Exception as e:
        logger.error(f"Unexpected error in stop_websocket: {str(e)}", exc_info=True)
        return Response({
            'error': 'Internal Server Error',
            'details': str(e),
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def check_websocket_status(request, user_id):
    """Проверяет статус WebSocket соединения через FastAPI"""
    logger.info(f"Checking WebSocket status for user {user_id}")

    try:
        response = requests.get(
            f'{HOST_FAST_API}/users/{user_id}/exchanges/binance/status/',
            timeout=5
        )

        if response.status_code == 200:
            return Response(response.json())
        else:
            return Response({
                'error': f'FastAPI error: {response.status_code}',
                'details': response.text
            }, status=response.status_code)

    except Exception as e:
        return Response({
            'error': 'Cannot check status',
            'details': str(e)
        }, status=500)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def create_trade(request):
    """Создание новой сделки через FastAPI WebSocket"""
    try:
        symbol = request.data.get('symbol', '').upper()
        usdt_amount = float(request.data.get('usdt_amount', 0))
        target_profit = float(request.data.get('target_profit', 1.0))
        stop_loss = float(request.data.get('stop_loss', 0.5))

        # Валидация
        if not symbol:
            return Response({'error': 'Symbol is required'}, status=400)

        if usdt_amount <= 0:
            return Response({'error': 'Amount must be positive'}, status=400)

        if target_profit <= 0 or stop_loss <= 0:
            return Response({'error': 'Target profit and stop loss must be positive'}, status=400)

        # Проверяем активное WebSocket соединение
        status_url = f'{HOST_FAST_API}/users/{request.user.id}/exchanges/binance/status/'
        try:
            status_response = requests.get(status_url, timeout=5)
            if status_response.status_code != 200:
                return Response({
                    'error': 'WebSocket not active',
                    'message': 'Please start WebSocket connection first'
                }, status=400)
        except:
            return Response({'error': 'Cannot connect to WebSocket service'}, status=503)

        # Отправляем запрос в FastAPI для создания сделки
        create_trade_url = f'{HOST_FAST_API}/users/{request.user.id}/exchanges/binance/create_trade/'

        payload = {
            'symbol': symbol,
            'usdt_amount': usdt_amount,
            'target_profit': target_profit,
            'stop_loss': stop_loss
        }

        try:
            response = requests.post(
                create_trade_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )

            if response.status_code == 200:
                result = response.json()
                return Response(result)
            else:
                return Response({
                    'error': f'FastAPI error: {response.status_code}',
                    'details': response.text
                }, status=response.status_code)

        except requests.exceptions.Timeout:
            return Response({'error': 'Request timeout'}, status=504)
        except requests.exceptions.ConnectionError:
            return Response({'error': 'Cannot connect to WebSocket service'}, status=503)

    except ValueError as e:
        return Response({'error': f'Invalid value: {str(e)}'}, status=400)
    except Exception as e:
        logger.error(f"Error creating trade: {str(e)}", exc_info=True)
        return Response({'error': str(e)}, status=500)


# views.py (исправьте get_active_trades)
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_active_trades(request):  # Убрать async
    """Получение активных сделок пользователя"""
    try:
        exchange = Exchange.objects.get(name='binance')
        trades = Trade.objects.filter(
            user=request.user,
            exchange=exchange,
            status__in=['open', 'active']
        ).exclude(quantity=0)

        serializer = TradeSerializer(trades, many=True)

        return Response({
            'count': trades.count(),
            'trades': serializer.data,
            'summary': {
                'total_investment': sum(t.buy_price * t.quantity for t in trades),
                'total_trades': trades.count(),
            }
        })

    except Exception as e:
        logger.error(f"Error getting active trades: {str(e)}", exc_info=True)
        return Response({'error': str(e)}, status=500)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_trade_history(request):
    """Получение истории сделок"""
    try:
        from .models import Trade, Exchange

        exchange = Exchange.objects.get(name='binance')
        trades = Trade.objects.filter(
            user=request.user,
            exchange=exchange
        ).order_by('-detected_at')

        serializer = TradeSerializer(trades, many=True)

        # Статистика
        total_trades = trades.count()
        profitable_trades = trades.filter(status='closed_profit').count()
        loss_trades = trades.filter(status='closed_loss').count()
        open_trades = trades.filter(status='open').count()

        total_profit = sum(t.actual_profit for t in trades if t.actual_profit)

        return Response({
            'statistics': {
                'total_trades': total_trades,
                'profitable_trades': profitable_trades,
                'loss_trades': loss_trades,
                'open_trades': open_trades,
                'success_rate': (profitable_trades / total_trades * 100) if total_trades > 0 else 0,
                'total_profit': total_profit,
            },
            'trades': serializer.data
        })

    except Exception as e:
        return Response({'error': str(e)}, status=500)