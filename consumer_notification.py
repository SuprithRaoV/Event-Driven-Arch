import pika
import json
from rabbitmq_config import RABBITMQ_HOST, EXCHANGE_NAME

def format_notification(data, routing_key):
    order_id = data.get("order_id", "Unknown")
    student_name = data.get("student_name", "Unknown Student")

    notifications = {
        "order-created": f"[{student_name}] Notification: Order {order_id} has been placed successfully.",
        "payment-success": f"[{student_name}] Notification: Payment for Order {order_id} was successful.",
        "payment-denied": f"[{student_name}] Notification: Payment for Order {order_id} was denied.",
        "order-fulfilled": f"[{student_name}] Notification: Order {order_id} has been fulfilled.",
        "order-shipped": f"[{student_name}] Notification: Order {order_id} has been shipped."
    }

    return notifications.get(routing_key, f"[{student_name}] Notification: Unknown event for Order {order_id}.")

def send_notification(ch, method, properties, body):
    data = json.loads(body)
    message = format_notification(data, method.routing_key)
    print(message)

def setup_notification_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    channel.queue_declare(queue="notification_queue")

    routing_keys = [
        "order-created",
        "payment-success",
        "payment-denied",
        "order-fulfilled",
        "order-shipped"
    ]

    for key in routing_keys:
        channel.queue_bind(exchange=EXCHANGE_NAME, queue="notification_queue", routing_key=key)

    channel.basic_consume(queue="notification_queue", on_message_callback=send_notification, auto_ack=True)

    print("Waiting for notification messages...")
    channel.start_consuming()

if __name__ == "__main__":
    setup_notification_consumer()
