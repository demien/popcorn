from celery.bin.base import Command


class BaseCommand(Command):
    ENV_VAR_PREFIX = 'POPCORN_'

    def change_default_setting(self):
        self.app.conf['CELERYD_AUTOSCALER'] = 'popcorn.apps.guard.autoshrink:Autoshrink'
