import multiprocessing
import json
import time
import os
import shutil
from hadrosaur import Project

basedir = 'tmp'
shutil.rmtree(basedir, ignore_errors=True)
proj = Project(basedir)


@proj.resource('test')
def compute_test_resource(ident, args, ctx):
    ctx.logger.debug('this should go into run.log')
    if args.get('throw_error'):
        raise RuntimeError("Error!")
    with open(os.path.join(ctx.subdir, 'hello.txt'), 'w') as fd:
        fd.write('hello world')
    return {'val': time.time()}


@proj.resource('always_error')
def throw_something(ident, args, ctx):
    ctx.logger.info('output here')
    raise RuntimeError('This is an error!')


@proj.resource('delayed')
def delay_stuff(ident, args, subdir):
    time.sleep(3)
    return {'val': time.time()}


def test_create_new_project_valid():
    assert os.path.isdir(basedir)


def test_create_resource_valid():
    res_dir = 'tmp/test'
    assert os.path.isdir(res_dir)


def test_fetch_valid():
    """
    Test a resource fetch with valid computation
    """
    res_dir = 'tmp/test'
    result = proj.fetch('test', 1)
    assert result['result']['val'] > 0
    entry_dir = os.path.join(res_dir, '1')
    assert os.path.isdir(entry_dir)
    paths = ['storage/hello.txt', 'result.json', 'run.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(os.path.join(result['paths']['storage'], 'hello.txt')) as fd:
        content = fd.read()
        assert content == 'hello world'
    with open(result['paths']['status']) as fd:
        status = fd.read()
    assert status == 'completed'
    with open(result['paths']['start_time']) as fd:
        start_time = int(fd.read())
    with open(result['paths']['end_time']) as fd:
        end_time = int(fd.read())
    assert start_time <= end_time
    with open(result['paths']['result']) as fd:
        assert json.load(fd)['val'] > 0
    with open(result['paths']['log']) as fd:
        assert 'this should go into run.log' in fd.read()


def test_fetch_py_err():
    """
    Test a resource fetch with an internal error getting raised
    """
    entry_dir = os.path.join(basedir, 'always_error', '1')
    result = proj.fetch('always_error', 1)
    assert result['result'] is None
    paths = ['error.log', 'status', 'run.log', 'start_time', 'end_time']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(result['paths']['log']) as fd:
        assert 'output here' in fd.read()
    with open(result['paths']['error']) as fd:
        assert 'This is an error!' in fd.read()
    with open(result['paths']['status']) as fd:
        status = fd.read()
    assert status == 'error'
    assert result['status'] == status
    with open(result['paths']['start_time']) as fd:
        start_time = int(fd.read())
    with open(result['paths']['end_time']) as fd:
        end_time = int(fd.read())
    assert start_time <= end_time


def test_refetch_precomputed_valid_cache():
    """
    Test a fetch of a resource that has already been computed, returning the cached results
    """
    result1 = proj.fetch('test', 1)
    result2 = proj.fetch('test', 1)
    # As these are timestamps, they would not be the same if this were recomputed
    assert result1['result']['val'] == result2['result']['val']


def test_refetch_precomputed_valid_recompute():
    """
    Test a fetch of a resource with a force recompute
    """
    result1 = proj.fetch('test', 1)
    result2 = proj.fetch('test', 1, recompute=True)
    # As these are timestamps, the new value will be later
    assert result1['result']['val'] <= result2['result']['val']


def test_refetch_precomputed_error():
    """
    Test a fetch of a resource with a force recompute where we throw an error
    on the new compute
    """
    result1 = proj.fetch('test', 1)
    result2 = proj.fetch('test', 2, {'throw_error': True})
    assert result1['status'] == 'completed'
    assert result1['start_time'] <= result1['end_time']
    assert result2['status'] == 'error'
    assert result2['start_time'] <= result2['end_time']


def test_col_status_valid():
    status = proj.status('test')
    assert status == {'counts': {'total': 2, 'pending': 1, 'completed': 1, 'error': 0, 'unknown': 0}}


def test_col_resource_status_valid():
    """
    Relies on test/1 being present
    """
    status = proj.status('test', 1)
    assert status == 'completed'


def test_fetch_log_valid():
    """
    Relies on test/1 being present
    """
    log = proj.fetch_log('test', 1)
    assert 'this should go into run.log' in log


def test_fetch_error_valid():
    """
    Relies on test/1 being present
    """
    error = proj.fetch_error('always_error', 1)
    assert "This is an error!" in error


def test_find_by_status():
    ids = proj.find_by_status('test', 'completed')
    assert ids == ['1', '2']


def test_fetch_delayed():
    """
    Test a delayed resource fetch pending status
    Keep this test last to avoid upfront sleeping.
    """
    entry_dir = os.path.join(basedir, 'delayed', '1')
    status_path = os.path.join(entry_dir, 'status')
    start_path = os.path.join(entry_dir, 'start_time')
    end_path = os.path.join(entry_dir, 'end_time')
    proc = multiprocessing.Process(target=proj.fetch, args=('delayed', 1), daemon=True)
    proc.start()
    time.sleep(0.25)  # Allow some time for the status to be written
    with open(status_path) as fd:
        status = fd.read()
    with open(start_path) as fd:
        start_time = int(fd.read())
    with open(end_path) as fd:
        end_time = fd.read()
    assert status == 'pending'
    assert start_time > 0
    assert end_time == ''
    time.sleep(3)
    with open(status_path) as fd:
        status = fd.read()
    with open(start_path) as fd:
        start_time = int(fd.read())
    with open(end_path) as fd:
        end_time = int(fd.read())
    assert status == 'completed'
    assert start_time <= end_time
