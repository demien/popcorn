import socket

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
print HUB_IP
BROKER_URL = 'redis://%s:6379/0' % HUB_IP
# HUB_IP = '192.168.1.52'
# BROKER_URL = 'redis://localhost:6379/0'
