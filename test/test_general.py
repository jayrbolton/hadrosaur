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
    entry_dir = os.path.join(res_dir, '1')
    assert os.path.isdir(entry_dir)
    paths = ['storage/hello.txt', 'result.json', 'run.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(os.path.join(result['paths']['storage'], 'hello.txt')) as fd:
        content = fd.read()
        assert content == 'hello world'
    with open(os.path.join(entry_dir, 'status.json')) as fd:
        status = json.load(fd)
    assert status['completed'] is True
    assert status['error'] is False
    assert status['pending'] is False
    assert status['start_time'] < status['end_time']
    assert result['result']['val'] > 0
    with open(os.path.join(entry_dir, 'result.json')) as fd:
        assert json.load(fd)['val'] > 0
    with open(os.path.join(entry_dir, 'run.log')) as fd:
        assert 'this should go into run.log' in fd.read()


def test_fetch_py_err():
    """
    Test a resource fetch with an internal error getting raised
    """
    entry_dir = os.path.join(basedir, 'always_error', '1')
    result = proj.fetch('always_error', 1)
    paths = ['error.log', 'status.json', 'run.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(os.path.join(entry_dir, 'run.log')) as fd:
        assert 'output here' in fd.read()
    with open(os.path.join(entry_dir, 'error.log')) as fd:
        assert 'This is an error!' in fd.read()
    with open(os.path.join(entry_dir, 'status.json')) as fd:
        status = json.load(fd)
    assert status['completed'] is False
    assert status['error'] is True
    assert status['pending'] is False
    assert status['start_time'] <= status['end_time']
    assert result['status'] == status
    assert result['result'] is None


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
    assert result1['status']['completed'] is True
    assert result1['status']['error'] is False
    assert result1['status']['pending'] is False
    assert result1['status']['start_time'] <= result1['status']['end_time']
    assert result2['status']['completed'] is False
    assert result2['status']['error'] is True
    assert result2['status']['pending'] is False
    assert result2['status']['start_time'] <= result2['status']['end_time']


def test_fetch_delayed():
    """
    Test a delayed resource fetch pending status
    Keep this test last to avoid upfront sleeping.
    """
    entry_dir = os.path.join(basedir, 'delayed', '1')
    status_path = os.path.join(entry_dir, 'status.json')
    proc = multiprocessing.Process(target=proj.fetch, args=('delayed', 1), daemon=True)
    proc.start()
    time.sleep(0.15)  # Allow some time for status.json to be written
    with open(status_path) as fd:
        status = json.load(fd)
    assert status['completed'] is False
    assert status['error'] is False
    assert status['pending'] is True
    assert status['start_time'] > 0
    assert status['end_time'] is None
    time.sleep(3)
    with open(status_path) as fd:
        status = json.load(fd)
    assert status['completed'] is True
    assert status['error'] is False
    assert status['pending'] is False
    assert status['start_time'] <= status['end_time']
