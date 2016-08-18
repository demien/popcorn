import socket

HEALTHY_MOCK = True

CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERYD_POOL_RESTARTS = True
CELERY_IMPORTS = (
    "tasks",
)
DEFAULT_QUEUE = {
    'popcorn': 'simple',
    'demien': 'simple',
}
HUB_IP = socket.gethostbyname(socket.gethostname())
HUB_IP = '172.17.0.2'
BROKER_URL = 'redis://%s:6379/0' % HUB_IP
