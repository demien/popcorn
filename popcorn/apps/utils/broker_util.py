from kombu import Connection
from celery.task.control import inspect

def taste_soup(queue):
    try:
        with Connection('redis://localhost:6379/0') as conn:
            q = conn.SimpleQueue(queue)
            return q.qsize()
    except Exception as e:
        return 0
