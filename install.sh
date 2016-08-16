# install celery
ln -s /var/popcorn/celery/celery /usr/local/bin/celery
chmod 744 /var/popcorn/celery/celery

# install popcorn
pip install -e /var/popcorn
