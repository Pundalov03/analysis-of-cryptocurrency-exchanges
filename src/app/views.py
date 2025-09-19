from django.contrib.auth import authenticate
from django.http import HttpResponse
import requests
from rest_framework import status
from rest_framework.authtoken.models import Token
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated, IsAdminUser
from rest_framework.response import Response

from .models import APIKey, Exchange
from .serializers import UserRegisterSerializer, UserLoginSerializer, ExchangeSerializer, APIKeySerializer

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
    token, created  = Token.objects.get_or_create(user=user)

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
        username = serializer.validated_data['username'],
        password = serializer.validated_data['password']
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
    try:
        try:
            binance_exchange = Exchange.objects.get(name='binance')
        except Exchange.DoesNotExist:
            return Response({
                'error': 'Exchange not found',
            }, status=status.HTTP_404_NOT_FOUND)

        try:
            api_keys = APIKey.objects.get(
                user_id=user_id,
                exchange=binance_exchange,
            )
        except APIKey.DoesNotExist:
            return Response({
                'error': 'API key for Binance not found. Please add API keys first using /add-api-key/ endpoint.',
            })

        api_key = api_keys.api_key
        secret_key = api_keys.secret_key

        if not api_key or not secret_key:
            return Response({
                'error': 'API key required'
            })

        fastapi_url = f'{HOST_FAST_API}/users/{user_id}/exchanges/binance/ws/start/'
        response = requests.post(
            fastapi_url,
            json={
                'api_key': api_key,
                'secret_key': secret_key,
            }
        )

        if response.status_code != 200:
            return Response({
                'error': f'FastAPI error: {response.status_code}',
                'details': response.text
            }, status=response.status_code)

        return Response(response.json(), status=response.status_code)
    except requests.exceptions.ConnectionError:
        return Response({
            'error': 'FastAPI server is not running',
            'message': 'Please, start the FastAPI server on port 8001',
        }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
    except Exception as e:
        return Response({
            'error': 'Internal Server Error',
            'details': str(e),
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

@api_view(['POST'])
@permission_classes([IsAuthenticated])
def stop_websocket(request, user_id):
    try:
        fastapi_url = f'{HOST_FAST_API}/users/{user_id}/exchanges/binance/ws/stop/'
        response = requests.post(fastapi_url)

        if response.status_code != 200:
            return Response({
                'error': f'FastAPI error: {response.status_code}',
                'details': response.text
            }, status=response.status_code)

        return Response(response.json())
    except requests.exceptions.ConnectionError:
        return Response({
            'error': 'FastAPI server is not running',
        }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
    except Exception as e:
        return Response({
            'error': 'Internal Server Error',
            'details': str(e),
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
