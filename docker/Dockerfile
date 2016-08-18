FROM python:2.7
MAINTAINER Demien "demien@appannie.com"

# Pip package
RUN pip install psutil Pyro4==4.45 kombu ipdb billiard pytz redis

# Install Redis
RUN apt-get update && \
    apt-get install -y redis-server \
    vim \
    sudo \
    curl \
    chkconfig
RUN echo "\n# bind ip\nbind 0.0.0.0\n" >> /etc/redis/redis.conf

# celery env
ENV PYTHONPATH /var/celery/
ENV PYRO_SERIALIZERS_ACCEPTED pickle

# init
CMD /var/popcorn/install.sh && bash

