import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME  # Importing constants

def fulfill_order(ch, method, properties, body):
    order = json.loads(body)
    order_id = order['order_id']
    student_name = order.get('student_name', 'Unknown Student')

    print(f"[{student_name}] Fulfillment: Order {order_id} fulfilled. Events published.")

    # Publish fulfillment event
    ch.basic_publish(exchange=EXCHANGE_NAME, routing_key='order-fulfilled', body=json.dumps(order))

def start_order_fulfillment_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declare exchange and queue
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    channel.queue_declare(queue='order-fulfillment')
    channel.queue_bind(exchange=EXCHANGE_NAME, queue='order-fulfillment', routing_key='payment-applied')

    channel.basic_consume(queue='order-fulfillment', on_message_callback=fulfill_order, auto_ack=True)

    print("Waiting for payment-applied events...")
    channel.start_consuming()

if __name__ == "__main__":
    start_order_fulfillment_consumer()
