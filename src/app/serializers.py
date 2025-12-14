from django.contrib.auth.models import User
from .models import Profile, Exchange, APIKey, Trade
from rest_framework import serializers

class UserRegisterSerializer(serializers.ModelSerializer):
    confirm_password = serializers.CharField(style={'input_type': 'password'}, write_only=True)

    class Meta:
        model = User
        fields = ('username', 'email', 'password', 'confirm_password')

        extra_kwargs = {
            'password': {'write_only': True},
            'email': {'required': True},
        }

    def validate(self, data):
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        confirm_password = data.get('confirm_password')

        errors = {}
        password_errors = []

        if User.objects.filter(username=username).exists():
            errors['username'] = 'User with this username already exists'

        if User.objects.filter(email=email).exists():
            errors['email'] = 'User with this email already exists'

        if password != confirm_password:
            password_errors.append('Passwords do not match')

        if len(password) < 8:
            password_errors.append('Password must be at least 8 characters long')

        if not any(char.isdigit() for char in password):
            password_errors.append('Password must contain at least one number')

        if not any(char for char in password if char == char.upper()):
            password_errors.append('Password must contain at least one uppercase letter')

        if not any(char for char in password if not char.isdigit() and not char.isalpha()):
            password_errors.append('Password must contain at least one special character')

        if password_errors:
            errors['password'] = password_errors

        if errors:
            raise serializers.ValidationError(errors)

        return data

    def create(self, validated_data):
        validated_data.pop('confirm_password')

        user = User.objects.create_user(**validated_data)

        Profile.objects.create(user=user, role='user')

        return user

class UserLoginSerializer(serializers.Serializer):
    username = serializers.CharField(required=True)
    password = serializers.CharField(style={'input_type': 'password'}, write_only=True, required=True)

    def validate(self, data):
        username = data.get('username')

        if not User.objects.filter(username=username).exists():
            raise serializers.ValidationError('User with this username does not exist')

        return data

class ExchangeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Exchange

        fields = ('name',)
        extra_kwargs = {
            'name': {'required': True},
        }

    def validate(self, data):
        if Exchange.objects.filter(name=data['name']).exists():
            raise serializers.ValidationError('Exchange with this name already exists')

        return data

    def create(self, validated_data):
        exchange = Exchange.objects.create(**validated_data)
        exchange.name = validated_data['name'].lower()
        exchange.save()

        return exchange

class APIKeySerializer(serializers.ModelSerializer):
    exchange_name = serializers.CharField(required=True)
    api_key = serializers.CharField(required=True)
    secret_key = serializers.CharField(write_only=True)

    class Meta:
        model = APIKey
        fields = ('exchange_name', 'api_key', 'secret_key')

    def validate(self, data):
        request = self.context.get('request')

        if not request.user.is_authenticated:
            raise serializers.ValidationError('User not authenticated')

        user = request.user

        try:
            exchange = Exchange.objects.get(name=data['exchange_name'])
            data.pop('exchange_name')
        except Exchange.DoesNotExist:
            raise serializers.ValidationError('Exchange with this name does not exist')

        if APIKey.objects.filter(user = user, exchange = exchange).exists():
            raise serializers.ValidationError('API key already exists')

        data['user'] = user
        data['exchange'] = exchange

        return data

    def create(self, validated_data):
        secret_key = validated_data.pop('secret_key')

        api_key = APIKey.objects.create(**validated_data)
        api_key.secret_key = secret_key
        api_key.save()

        return api_key

class TradeSerializer(serializers.ModelSerializer):
    class Meta:
        model = Trade
        fields = (
            'id', 'symbol', 'quantity', 'buy_price',
            'target_profit_percent', 'stop_loss_percent', 'status'
        )
        extra_kwargs = {
            'target_profit_percent': {'required': False, 'default': 1.0},
            'stop_loss_percent': {'required': False, 'default': 0.5},
        }