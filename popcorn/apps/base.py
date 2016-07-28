class BaseApp(object):

    def setup_defaults(self, loglevel=None, logfile=None, **_kw):
        self.loglevel = self._getopt('log_level', loglevel)
        self.logfile = self._getopt('log_file', logfile)

    def _getopt(self, key, value):
        if value is not None:
            return value
        return self.app.conf.find_value_for_key(key, namespace='celeryd')
