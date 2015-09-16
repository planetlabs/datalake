
_conf = {}


def get_config():
    return _conf


def get_config_var(name):
    return _conf.get(name)


def set_config(conf):
    global _conf
    _conf = conf
