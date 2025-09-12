from django.contrib import admin
from .models import Profile, Exchange, APIKey, Trade, Report

@admin.register(Profile)
class ProfileAdmin(admin.ModelAdmin):
    list_display = [
        'user',
        'role',
    ]

@admin.register(Exchange)
class ExchangeAdmin(admin.ModelAdmin):
    list_display = [
        'name',
    ]

@admin.register(APIKey)
class APIKeyAdmin(admin.ModelAdmin):
    list_display = [
        'user',
        'exchange',
        'api_key',
        'created_at',
    ]

@admin.register(Trade)
class TradeAdmin(admin.ModelAdmin):
    list_display = [
        'user',
        'exchange',
        'symbol',
        'quantity',
        'buy_price',
        'sell_price',
        'estimated_profit',
        'detected_at',
    ]

@admin.register(Report)
class ReportAdmin(admin.ModelAdmin):
    list_display = [
        'user',
        'exchange',
        'title',
        'status',
        'format',
        'created_at',
    ]