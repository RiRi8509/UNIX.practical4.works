import json
import os
import time

import pika


BROKER_HOST = os.getenv("BROKER_HOST", "broker")
QUEUE_NAME = os.getenv("QUEUE_NAME", "tasks")
TASK_INTERVAL = float(os.getenv("TASK_INTERVAL", "1"))


def connect_to_broker():
    while True:
        try:
            parameters = pika.ConnectionParameters(host=BROKER_HOST, heartbeat=30)
            return pika.BlockingConnection(parameters)
        except pika.exceptions.AMQPConnectionError:
            print("Producer: broker is unavailable, retrying...", flush=True)
            time.sleep(2)


def main():
    task_number = 1

    while True:
        connection = connect_to_broker()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        try:
            while True:
                task = {"id": task_number, "number": task_number}
                channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_NAME,
                    body=json.dumps(task),
                    properties=pika.BasicProperties(delivery_mode=2),
                )
                print(
                    f"Producer: sent task {task_number}, number={task_number}",
                    flush=True,
                )
                task_number += 1
                time.sleep(TASK_INTERVAL)
        except pika.exceptions.AMQPError:
            print("Producer: connection lost, reconnecting...", flush=True)
        finally:
            if connection.is_open:
                connection.close()


if __name__ == "__main__":
    main()
