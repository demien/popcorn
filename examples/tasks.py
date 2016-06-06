import time
from celery import task

@task
def add(x, y):
    return x + y

@task
def say(something):
    time.sleep(0.1)
    print something
