# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import os
from subprocess import check_output, STDOUT, CalledProcessError
from locale import getpreferredencoding


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
            m += e.output.decode(getpreferredencoding(False))
        raise CreationTimeError(m)


DEFAULT_CRTIME_FACILITY = _crtime_linux


def get_crtime(f, crtime_facility=DEFAULT_CRTIME_FACILITY):
    '''get the creation time of a file

    return the creation time of the file in seconds since the epoch.

    Note that for testing purposes you can inject a different crtime_facility.
    '''
    return crtime_facility(f)
