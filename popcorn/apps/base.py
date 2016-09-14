import logging

LOGLEVEL = None

class BaseApp(object):

    def init(self, **kwargs):
        self.init_log()

    def init_log(self, **kwargs):
        self.on_init_blueprint(**kwargs)

    def setup_instance(self, **kwargs):
        self.no_color = None

    def on_init_blueprint(self, **kwargs):
        self._custom_logging = self.setup_logging()

    def setup_logging(self, colorize=None):
        if colorize is None and self.no_color is not None:
            colorize = not self.no_color
        return self.app.log.setup(self.loglevel, self.logfile, redirect_stdouts=False, colorize=colorize)

    def setup_defaults(self, loglevel=None, logfile=None, **_kw):
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)

    def _getopt(self, key, value):
        if value is not None:
            return value
        return self.app.conf.find_value_for_key(key, namespace='celeryd')
