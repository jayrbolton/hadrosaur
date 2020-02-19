import json
import time
import os
import shutil
import multiprocessing
from uuid import uuid4
from hadrosaur import Project

basedir = 'tmp'
shutil.rmtree(basedir, ignore_errors=True)
proj = Project(basedir)


@proj.collection('test')
def compute_test_resource(ident, args, ctx):
    ctx.logger.debug('this should go into run.log')
    if args.get('throw_error'):
        raise RuntimeError("Error!")
    with open(os.path.join(ctx.subdir, 'hello.txt'), 'w') as fd:
        fd.write('hello world')
    return {'val': time.time()}


@proj.collection('always_error')
def throw_something(ident, args, ctx):
    ctx.logger.info('output here')
    raise RuntimeError('This is an error!')


@proj.collection('delayed')
def delay_stuff(ident, args, subdir):
    time.sleep(1)
    return {'val': time.time()}


def test_create_new_project_valid():
    assert os.path.isdir(basedir)


def test_create_resource_valid():
    res_dir = 'tmp/test'
    assert os.path.isdir(res_dir)


def test_fetch_blocking_valid():
    """
    Test a resource fetch with valid computation
    """
    res_dir = 'tmp/test'
    ident = _id()
    res = proj.fetch('test', ident)
    assert res.result['val'] > 0
    entry_dir = os.path.join(res_dir, ident)
    assert os.path.isdir(entry_dir)
    paths = ['storage/hello.txt', 'result.json', 'run.log', 'error.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(os.path.join(res.paths['storage'], 'hello.txt')) as fd:
        content = fd.read()
        assert content == 'hello world'
    with open(res.paths['status']) as fd:
        status = fd.read()
    assert status == 'complete'
    with open(res.paths['start_time']) as fd:
        start_time = int(fd.read())
    with open(res.paths['end_time']) as fd:
        end_time = int(fd.read())
    assert start_time <= end_time
    with open(res.paths['result']) as fd:
        assert json.load(fd)['val'] > 0
    with open(res.paths['log']) as fd:
        assert 'this should go into run.log' in fd.read()


def test_fetch_py_err():
    """
    Test a resource fetch with an internal error getting raised
    """
    ident = _id()
    entry_dir = os.path.join(basedir, 'always_error', ident)
    res = proj.fetch('always_error', ident)
    assert res.result is None
    paths = ['error.log', 'status', 'run.log', 'start_time', 'end_time']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(res.paths['log']) as fd:
        assert 'output here' in fd.read()
    with open(res.paths['error']) as fd:
        assert 'This is an error!' in fd.read()
    with open(res.paths['status']) as fd:
        status = fd.read()
    assert status == 'error'
    assert res.status == status
    with open(res.paths['start_time']) as fd:
        start_time = int(fd.read())
    with open(res.paths['end_time']) as fd:
        end_time = int(fd.read())
    assert start_time <= end_time


def test_refetch_precomputed_valid_cache():
    """
    Test a fetch of a resource that has already been computed, returning the cached results
    """
    ident = _id()
    res1 = proj.fetch('test', ident)
    res2 = proj.fetch('test', ident)
    # As these are timestamps, they would not be the same if this were recomputed
    assert res1.result['val'] == res2.result['val']


def test_refetch_precomputed_valid_recompute():
    """
    Test a fetch of a resource with a force recompute
    """
    ident = _id()
    res1 = proj.fetch('test', ident)
    res2 = proj.fetch('test', ident, recompute=True)
    # As these are timestamps, the new value will be later
    assert res1.result['val'] <= res2.result['val']


def test_refetch_precomputed_error():
    """
    Test a fetch of a resource with a force recompute where we throw an error
    on the new compute
    """
    ident1 = _id()
    ident2 = _id()
    result1 = proj.fetch('test', ident1)
    result2 = proj.fetch('test', ident2, args={'throw_error': True})
    assert result1.status == 'complete'
    assert result1.start_time <= result1.end_time
    assert result2.status == 'error'
    assert result2.start_time <= result2.end_time


def test_col_status_valid():
    ident1 = _id()
    ident2 = _id()
    proj.fetch('test', ident1)
    proj.fetch('test', ident2, args={'throw_error': True})
    status = proj.stats('test')
    counts = status['counts']
    assert counts['total'] >= 2
    assert counts['complete'] >= 1
    assert counts['error'] >= 1


def test_status_valid():
    ident1 = _id()
    ident2 = _id()
    proj.fetch('test', ident1)
    proj.fetch('always_error', ident2)
    proc = multiprocessing.Process(target=proj.fetch, args=('delayed', ident2), daemon=True)
    proc.start()
    # Allow time for the delayed resource to be pending
    time.sleep(0.15)
    status = proj.stats()
    assert status['test']['counts']['complete'] >= 1
    assert status['delayed']['counts']['pending'] >= 1
    assert status['always_error']['counts']['error'] >= 1


def test_col_resource_status_valid():
    """
    Relies on test/1 being present
    """
    ident = _id()
    status = proj.fetch('test', ident)
    status = proj.status('test', ident)
    assert status == 'complete'


def test_fetch_log_valid():
    """
    Relies on test/1 being present
    """
    ident = _id()
    proj.fetch('test', ident)
    log = proj.fetch_log('test', ident)
    assert 'this should go into run.log' in log


def test_fetch_error_valid():
    """
    Relies on test/1 being present
    """
    ident = _id()
    proj.fetch('always_error', ident)
    error = proj.fetch_error('always_error', ident)
    assert "This is an error!" in error


def test_find_by_status():
    ident = _id()
    proj.fetch('test', ident)
    ids = proj.find_by_status('test', 'complete')
    assert ident in set(ids)


def test_fetch_delayed():
    """
    Test a delayed resource fetch pending status
    Keep this test last to avoid upfront sleeping.
    """
    ident = _id()
    proc = multiprocessing.Process(target=proj.fetch, args=('delayed', ident), daemon=True)
    proc.start()
    time.sleep(0.1)
    status = proj.status('delayed', ident)
    assert status == 'pending'
    time.sleep(1.25)
    res = proj.fetch('delayed', ident)
    assert res.status == 'complete'
    assert res.start_time < res.end_time
    assert res.result


def _id():
    return str(uuid4())
