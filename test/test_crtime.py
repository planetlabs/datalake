import pytest
import os
import stat
from datalake_common.tests import tmpfile
import time

from datalake.crtime import get_crtime, CreationTimeError


crtime = os.environ.get('CRTIME', '/usr/local/bin/crtime')
crtime_available = os.path.isfile(crtime) and os.access(crtime, os.X_OK)
crtime_setuid = False
if crtime_available:
    s = os.stat(crtime)
    crtime_setuid = s.st_mode & stat.S_ISUID and s.st_uid == 0


def test_crtime_does_not_exist(monkeypatch, tmpfile):
    monkeypatch.setenv('CRTIME', '/no/such/crtime')
    f = tmpfile('foobar')
    with pytest.raises(CreationTimeError):
        get_crtime(f)


def test_fails_if_file_does_not_exist():
    with pytest.raises(IOError):
        get_crtime('/blurb/nosuchfile')


@pytest.mark.skipif(not crtime_setuid, reason='crtime required')
def test_crtime_works(tmpfile):
    f = tmpfile('foobar')
    t = get_crtime(f)
    error = abs(t - time.time())
    assert error <= 1
