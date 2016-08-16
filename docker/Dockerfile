FROM python:2.7
MAINTAINER Demien "demien@appannie.com"

# Pip package
RUN pip install psutil Pyro4 kombu ipdb billiard pytz

# Install Redis
RUN apt-get update && \
    apt-get install -y redis-server \
    vim \
    sudo \
    curl \
    chkconfig
# RUN update-rc.d redis-server defaults
RUN echo -e "# start redis-server\nservice redis-server start\n" >> /etc/bash.bashrc

# celery env
ENV PYTHONPATH /var/celery/
