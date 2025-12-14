from django.urls import path

from . import views

urlpatterns = [
    path('register/', views.register, name='register'),
    path('login/', views.login, name='login'),
    path('logout/', views.logout, name='logout'),

    path('add-exchange/', views.add_exchange, name='add_exchange'),
    path('add-api-key/', views.add_api_key, name='add_api_key'),

    path('users/<int:user_id>/exchanges/binance/ws/start/', views.start_websocket, name='start_websocket'),
    path('users/<int:user_id>/exchanges/binance/ws/stop/', views.stop_websocket, name='stop_websocket'),

    path('trades/create/', views.create_trade, name='create_trade'),
    path('trades/active/', views.get_active_trades, name='active_trades'),
    path('trades/close/<int:trade_id>/', views.close_trade, name='close_trade'),
    path('trades/history/', views.get_trade_history, name='trade_history'),
]
