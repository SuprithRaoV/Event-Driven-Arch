
import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def ship_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['order_id']
    student_name = order.get('student_name', 'Unknown Student')

    print(f"[{student_name}] Shipping: Order {order_id} shipped. Events published.")

    # Publish shipping event
    ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='order-shipped', body=json.dumps(order))

def start_shipping_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare exchange and queue
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    channel.queue_declare(queue='shipping')
    channel.queue_bind(exchange=EXCHANGE_NAME, queue='shipping', routing_key='order-fulfilled')

    channel.basic_consume(queue='shipping', on_message_callback=ship_order, auto_ack=True)

    print("Waiting for order-fulfilled events...")
    channel.start_consuming()

if __name__ == "__main__":
    start_shipping_consumer()
