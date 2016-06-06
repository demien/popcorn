from celery.bin.base import Command


class BaseCommand(Command):
    ENV_VAR_PREFIX = 'POPCORN_'
