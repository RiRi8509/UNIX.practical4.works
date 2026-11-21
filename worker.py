import json
import os
import signal
import socket
import time

import pika


BROKER_HOST = os.getenv("BROKER_HOST", "broker")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
PROCESSING_TIME = float(os.getenv("PROCESSING_TIME", "1"))
WORKER_ID = socket.gethostname()
STOP_REQUESTED = False


def handle_sigterm(_signum, _frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    print(f"Worker {WORKER_ID}: SIGTERM received, stopping...", flush=True)


def connect_to_broker():
    while not STOP_REQUESTED:
        try:
            parameters = pika.ConnectionParameters(host=BROKER_HOST, heartbeat=30)
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            print(f"Worker {WORKER_ID}: broker is unavailable, retrying...", flush=True)
            time.sleep(2)

    return None


def process_task(channel, method, properties, body):
    del properties

    task = json.loads(body)
    task_id = task["id"]
    number = task["number"]

    time.sleep(PROCESSING_TIME)
    result = number * number

    print(
        f"Worker {WORKER_ID}: task {task_id}, {number}^2={result}",
        flush=True,
    )
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)

    while not STOP_REQUESTED:
        connection = connect_to_broker()
        if connection is None:
            break

        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Лимит применяется отдельно к каждому потребителю: пока работник не
        # подтвердил текущее задание, RabbitMQ не выдаёт ему следующее.
        channel.basic_qos(prefetch_count=1, global_qos=False)

        # Ручное подтверждение включено явно. Сообщение считается выполненным
        # только после вызова basic_ack() в функции process_task().
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=process_task,
            auto_ack=False,
        )

        print(f"Worker {WORKER_ID}: waiting for tasks", flush=True)

        try:
            # time_limit не даёт ожиданию блокировать проверку STOP_REQUESTED.
            while not STOP_REQUESTED:
                connection.process_data_events(time_limit=1)
        except pika.exceptions.AMQPError:
            if STOP_REQUESTED:
                break
            print(f"Worker {WORKER_ID}: connection lost, reconnecting...", flush=True)
        finally:
            if connection.is_open:
                connection.close()

    print(f"Worker {WORKER_ID}: stopped", flush=True)


if __name__ == "__main__":
    main()
