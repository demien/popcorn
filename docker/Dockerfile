FROM python:2.7
MAINTAINER Demien "demien@appannie.com"

# Pip package
RUN pip install psutil Pyro4==4.47 kombu ipdb billiard pytz redis vine mock

# Install Redis
RUN apt-get update && \
    apt-get install -y redis-server \
    vim \
    sudo \
    curl \
    chkconfig \
    htop
RUN echo "\n# bind ip\nbind 0.0.0.0\n" >> /etc/redis/redis.conf

# celery env
ENV PYTHONPATH /var/celery/
ENV PYRO_SERIALIZERS_ACCEPTED pickle

# init
CMD /var/popcorn/install.sh && bash

