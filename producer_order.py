import pika
import json
import random
import uuid
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def create_order():
    return {
        "order_id": str(uuid.uuid4())[:8],
        "user_id": f"u{random.randint(100, 999)}",
        "book_id": f"b{random.randint(1000, 9999)}",
        "student_name": "Suprith Rao"
    }

def publish_order(order):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')

    channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key="order-created",
        body=json.dumps(order)
    )
    print(f"[Suprith Rao] Producer: Order placed: {order}")

    connection.close()

if __name__ == "__main__":
    new_order = create_order()
    publish_order(new_order)
