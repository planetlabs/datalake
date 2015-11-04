import os
from subprocess import check_output, STDOUT, CalledProcessError


class CreationTimeError(Exception):
    pass


def _crtime_linux(f):
    '''get creation time using crtime utility

    This is quite hard to do on linux. So we use:

    https://github.com/planetlabs/crtime/

    Note that crtime must be setuid root for this to work.
    '''
    crtime = os.environ.get('CRTIME', '/usr/local/bin/crtime')
    if not os.path.exists(f):
        raise IOError('No such file ' + f)

    cmd = crtime + ' ' + f
    try:
        o = check_output(cmd, stderr=STDOUT, shell=True)
        return int(o)
    except CalledProcessError as e:
        if e.returncode == 13:
            m = '"' + cmd + '" failed. Permission denied. '
            m += 'Perhaps you need to setuid root on crtime?' 
        else:
            m = '"' + cmd + '" failed with code ' + str(e.returncode) + ': '
            m += e.output
        raise CreationTimeError(m)


DEFAULT_CRTIME_FACILITY=_crtime_linux


def get_crtime(f, crtime_facility=DEFAULT_CRTIME_FACILITY):
    '''get the creation time of a file

    return the creation time of the file in seconds since the epoch.

    Note that for testing purposes you can inject a different crtime_facility.
    '''
    return crtime_facility(f)
