"""Task 7: job queue in RabbitMQ. The metadata service puts jobs in it, and the workers take them."""
import json
import os
import time

import pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "")      # not set: no queue, the metadata service does the jobs itself
ENABLED = bool(RABBITMQ_URL)
QUEUE = "jobs"
PRIORITY = {"repair": 9, "rebalance": 5, "gc": 1, "scrub": 1}   # repairs go first


def _channel():
    while True:
        try:
            conn = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
            break
        except pika.exceptions.AMQPConnectionError:
            print("waiting for RabbitMQ...", flush=True)
            time.sleep(3)
    ch = conn.channel()
    ch.queue_declare(QUEUE, durable=True, arguments={"x-max-priority": 10})
    return conn, ch


def publish(jobs):
    """Put a list of jobs (dicts) in the queue."""
    if not jobs:
        return
    conn, ch = _channel()
    for job in jobs:
        ch.basic_publish("", QUEUE, json.dumps(job),
                         pika.BasicProperties(delivery_mode=2, priority=PRIORITY.get(job["type"], 1)))
    conn.close()


def waiting():
    """Number of jobs in the queue that no worker has taken yet."""
    conn, ch = _channel()
    count = ch.queue_declare(QUEUE, durable=True, arguments={"x-max-priority": 10}).method.message_count
    conn.close()
    return count


def consume(handle):
    """Call handle(job) for each job. RabbitMQ shares the jobs between all the running workers."""
    conn, ch = _channel()
    ch.basic_qos(prefetch_count=1)                 # take a new job only after finishing the last one

    def on_message(ch, method, properties, body):
        try:
            handle(json.loads(body))
        except Exception as e:
            print("job failed:", body[:200], e, flush=True)
        ch.basic_ack(method.delivery_tag)          # done: RabbitMQ can forget this job

    ch.basic_consume(QUEUE, on_message)
    ch.start_consuming()
