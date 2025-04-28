# Event-Driven Architecture with RabbitMQ

This project demonstrates an event-driven architecture using RabbitMQ as a message broker. It consists of a producer (`producer_order.py`) that sends messages to RabbitMQ, and two consumers (`consumer_payment.py` and `consumer_notification.py`) that process the messages asynchronously.

## Prerequisites

Ensure you have the following installed:
- Python 3.x
- RabbitMQ
- `pika` library for Python

## Installation

```bash
# Install pika (Python client for RabbitMQ)
pip install pika

# Install and set up RabbitMQ
# Download and install Erlang: https://www.erlang.org/downloads
# Download and install RabbitMQ: https://www.rabbitmq.com/docs/install-windows#installer
```

## Running the Application

```bash
# Start RabbitMQ service
rabbitmq-server start

# Run the producer to send order messages
python producer_order.py

# Run the consumers to process messages
python consumer_payment.py
python consumer_notification.py
```

## Project Structure

```plaintext
📦 event-driven-architecture
├── producer_order.py         # Sends order messages to RabbitMQ
├── consumer_payment.py       # Processes payment events
├── consumer_notification.py  # Sends notifications upon event completion
├── README.md                 # Project documentation
```

## How It Works

- The `producer_order.py` script publishes order messages to RabbitMQ.
- The `consumer_payment.py` script listens for payment processing requests and handles them.
- The `consumer_notification.py` script listens for completed transactions and sends notifications.

## License

This project is open-source and available for use under the MIT License.

## Author

[Your Name] - [Your GitHub Profile]



