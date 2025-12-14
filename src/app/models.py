from cryptography.fernet import Fernet
from django.contrib.auth.models import User
from django.db import models

from django.conf import settings


class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='profile')

    ROLE_CHOICES = [
        ('user', 'User'),
        ('admin', 'Administrator'),
        ('analyst', 'Analyst'),
    ]

    role = models.CharField(max_length=10, choices=ROLE_CHOICES, default='user')

    def __str__(self):
        return f'{self.user.username} - {self.role}'

class Exchange(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return f'{self.name}'

class APIKey(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    api_key = models.CharField(max_length=255)
    _secret_key = models.BinaryField()

    created_at = models.DateTimeField(auto_now_add=True)

    @property
    def secret_key(self):
        try:
            secret_data = self._secret_key
            if hasattr(secret_data, 'tobytes'):
                secret_data = secret_data.tobytes()

            cipher_suite = Fernet(settings.ENCRYPTION_KEY)
            decrypted = cipher_suite.decrypt(secret_data)
            return decrypted.decode()
        except Exception as e:
            raise e

    @secret_key.setter
    def secret_key(self, value):
        cipher_suite = Fernet(settings.ENCRYPTION_KEY)
        self._secret_key = cipher_suite.encrypt(value.encode())

    def __str__(self):
        return f'{self.user.username} - {self.exchange.name}'


class Trade(models.Model):
    STATUS_OPEN = 'open'
    STATUS_CLOSED_PROFIT = 'closed_profit'
    STATUS_CLOSED_LOSS = 'closed_loss'
    STATUS_CANCELLED = 'cancelled'

    STATUS_CHOICES = [
        (STATUS_OPEN, 'Open'),
        (STATUS_CLOSED_PROFIT, 'Closed with Profit'),
        (STATUS_CLOSED_LOSS, 'Closed with Loss'),
        (STATUS_CANCELLED, 'Cancelled'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    symbol = models.CharField(max_length=20)
    quantity = models.FloatField()
    buy_price = models.FloatField()
    sell_price = models.FloatField(null=True, blank=True)
    estimated_profit = models.FloatField(null=True, blank=True)

    target_profit_percent = models.FloatField(default=1.0)
    stop_loss_percent = models.FloatField(default=0.5)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_OPEN
    )
    closed_at = models.DateTimeField(null=True, blank=True)
    actual_profit = models.FloatField(null=True, blank=True)
    commission_paid = models.FloatField(default=0.0)
    buy_order_id = models.CharField(max_length=50, blank=True, null=True)
    close_reason = models.CharField(max_length=50, blank=True, null=True)

    detected_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'{self.user.username} - {self.symbol} - {self.status}'

    @property
    def is_active(self):
        """Алиас для проверки активности"""
        return self.status == self.STATUS_OPEN

    def calculate_current_profit(self, current_price: float) -> dict:
        """Расчет текущей прибыли"""
        if not current_price or not self.buy_price:
            return {'profit': 0, 'percent': 0}

        profit = (current_price - self.buy_price) * self.quantity
        percent = ((current_price - self.buy_price) / self.buy_price) * 100

        return {
            'profit': profit,
            'percent': percent,
            'current_price': current_price
        }
class Report(models.Model):
    FORMAT_CHOICES = [
        ('pdf', 'PDF'),
        ('excel', 'Excel'),
    ]

    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('processing', 'Processing'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE)
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE)
    title = models. CharField(max_length=200)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='pending')
    format = models.CharField(max_length=10, choices=FORMAT_CHOICES, default='excel')

    file = models.FileField(upload_to='reports/%Y/%m/%d/', blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f'{self.user.username} - {self.title} - {self.exchange.name} - {self.format}'