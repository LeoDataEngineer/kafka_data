from datetime import timezone
from random import randint
from confluent_kafka import Producer
from faker import Faker
import pandas as pd
import socket

# instanciar un objeto Faker para generar datos ficticios
fake = Faker()

# Instanciar un objeto Producer a partir de un diccionario de configuracion
# con los datos necesarios para conectarse
kafka_config = {
    'bootstrap.servers': 'dashing-ibex-6806-us1-kafka.upstash.io:9092',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'ZGFzaGluZy1pYmV4LTY4MDYkFoaV9RQtF1nv3j4u15zZ7mY9RJStxIlXkKOyIyE',
    'sasl.password': 'MjM4YjZhYTktMzc1MC00MzJhLWEyNTAtMjVhOWUwMzM5OGY2',
    'client.id': socket.gethostname()
}

producer = Producer(kafka_config)

def generate_fake_transaction(fake):
    """
    Generar una transacción ficticia de tarjeta de crédito

    Args:
        fake: instancia de Faker
    Returns:
        dict: transacción ficticia
    """
    transaction = {
        'cc_no': fake.credit_card_number(card_type=None),
        'cc_provider': fake.credit_card_provider(card_type=None),
        'cc_expire': fake.credit_card_expire(start='now', end='+10y', date_format='%m/%y'),
        'amount': fake.pydecimal(left_digits=2, right_digits=2, positive=True),
        'currency': 'USD',
        'merchant': fake.company(),
        'timestamp': fake.date_time_between(start_date='-6m', end_date='now', tzinfo=timezone.utc).isoformat()
    }
    return transaction

def delivery_callback(err, msg):
    if err is not None:
        print(f'Message failed delivery: {err}')
    else:
        print(f'Message delivered to {msg.topic()}: {msg.value()}')

for i in range(randint(1, 5)):
    transaction_record = generate_fake_transaction(fake)
    cc_no = transaction_record["cc_no"]
    cc_provider = transaction_record["cc_provider"]
    cc_expire = transaction_record["cc_expire"]
    amount = transaction_record["amount"]
    currency = transaction_record["currency"]
    merchant = transaction_record["merchant"]
    timestamp = transaction_record["timestamp"]

    # Convertir a formato de fecha y hora
    timestamp = pd.to_datetime(timestamp)

    # Formatear como deseas
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

    transaction_record = f"{cc_no} - {cc_provider} - {cc_expire} - {amount} - {currency} - {merchant} - {timestamp}"

    producer.produce(
        'transaccion_tc',
        transaction_record.encode('utf-8'),
        callback=delivery_callback
    )

# Espera a que todos los mensajes sean entregados
producer.flush()



        

