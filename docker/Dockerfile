FROM python:2.7
MAINTAINER Demien "demien@appannie.com"

# Pip package
RUN pip install psutil Pyro4 kombu ipdb billiard pytz

# Install Redis
RUN apt-get update && \
    apt-get install -y redis-server
# RUN update-rc.d redis_server defaults

# Install celery
# RUN ln -s /var/popcorn/celery/celery /usr/local/bin/celery
# RUN chmod 744 /var/popcorn/celery/celery
# ENV PYTHONPATH /var/celery/
