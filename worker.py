import json
import os
import socket
import time

import pika


BROKER_HOST = os.getenv("BROKER_HOST", "broker")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
PROCESSING_TIME = float(os.getenv("PROCESSING_TIME", "1"))
WORKER_ID = socket.gethostname()


def connect_to_broker():
    while True:
        try:
            parameters = pika.ConnectionParameters(host=BROKER_HOST, heartbeat=30)
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            print(f"Worker {WORKER_ID}: broker is unavailable, retrying...", flush=True)
            time.sleep(2)


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
    while True:
        connection = connect_to_broker()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Один работник получает только одно неподтверждённое задание за раз.
        # Поэтому RabbitMQ равномерно раздаёт задания свободным репликам.
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_task)

        print(f"Worker {WORKER_ID}: waiting for tasks", flush=True)

        try:
            channel.start_consuming()
        except pika.exceptions.AMQPError:
            print(f"Worker {WORKER_ID}: connection lost, reconnecting...", flush=True)
        finally:
            if connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
