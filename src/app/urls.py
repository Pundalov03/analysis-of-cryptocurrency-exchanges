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
]
