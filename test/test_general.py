import multiprocessing
import json
import sys
import time
import os
import shutil
from hadrosaur import Project

basedir = 'tmp'
shutil.rmtree(basedir, ignore_errors=True)
proj = Project(basedir)


@proj.resource('test')
def compute_test_resource(ident, args, subdir):
    print('this should go into stdout.log')
    sys.stderr.write('this should go into stderr.log\n')
    if args.get('throw_error'):
        raise RuntimeError("Error!")
    with open(os.path.join(subdir, 'hello.txt'), 'w') as fd:
        fd.write('hello world')
    return {'val': time.time()}


@proj.resource('always_error')
def throw_something(ident, args, subdir):
    print('stdout here')
    sys.stderr.write('stderr here\n')
    raise RuntimeError('This is an error!')


@proj.resource('delayed')
def delay_stuff(ident, args, subdir):
    time.sleep(3)
    return {'val': time.time()}


def test_create_new_project_valid():
    assert os.path.exists(basedir)
    assert os.path.isdir(basedir)


def test_create_resource_valid():
    res_dir = 'tmp/test'
    assert os.path.exists(res_dir)
    assert os.path.isdir(res_dir)


def test_fetch_valid():
    """
    Test a resource fetch with valid computation
    """
    res_dir = 'tmp/test'
    result = proj.fetch('test', 1)
    entry_dir = os.path.join(res_dir, '1')
    assert os.path.exists(entry_dir)
    assert os.path.isdir(entry_dir)
    with open(os.path.join(entry_dir, 'hello.txt')) as fd:
        content = fd.read()
        assert content == 'hello world'
    paths = ['hello.txt', 'result.json', 'stdout.log', 'status.json', 'stderr.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    expected_status = {'completed': True, 'error': False, 'pending': False}
    with open(os.path.join(entry_dir, 'status.json')) as fd:
        status = json.load(fd)
        assert status == expected_status
    assert result['status'] == expected_status
    assert result['result']['val'] > 0
    with open(os.path.join(entry_dir, 'result.json')) as fd:
        assert json.load(fd)['val'] > 0
    with open(os.path.join(entry_dir, 'stdout.log')) as fd:
        assert fd.read() == 'this should go into stdout.log\n'
    with open(os.path.join(entry_dir, 'stderr.log')) as fd:
        assert fd.read() == 'this should go into stderr.log\n'


def test_fetch_py_err():
    """
    Test a resource fetch with an internal error getting raised
    """
    entry_dir = os.path.join(basedir, 'always_error', '1')
    result = proj.fetch('always_error', 1)
    paths = ['error.log', 'status.json', 'stdout.log', 'stderr.log']
    for p in paths:
        assert os.path.exists(os.path.join(entry_dir, p))
    with open(os.path.join(entry_dir, 'stdout.log')) as fd:
        assert fd.read() == 'stdout here\n'
    with open(os.path.join(entry_dir, 'stderr.log')) as fd:
        assert fd.read() == 'stderr here\n'
    with open(os.path.join(entry_dir, 'error.log')) as fd:
        assert 'This is an error!' in fd.read()
    expected_status = {'completed': False, 'pending': False, 'error': True}
    with open(os.path.join(entry_dir, 'status.json')) as fd:
        status = json.load(fd)
        assert status == expected_status
    assert result['status'] == expected_status
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
    print('result1 result2', result1, result2)
    # As these are timestamps, the new value will be later
    assert result1['result']['val'] < result2['result']['val']


def test_refetch_precomputed_error():
    """
    Test a fetch of a resource with a force recompute where we throw an error
    on the new compute
    """
    result1 = proj.fetch('test', 1)
    result2 = proj.fetch('test', 2, {'throw_error': True})
    assert result1['status'] == {'completed': True, 'error': False, 'pending': False}
    assert result2['status'] == {'completed': False, 'error': True, 'pending': False}


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
        assert json.load(fd) == {'pending': True, 'completed': False, 'error': False}
    time.sleep(3)
    with open(status_path) as fd:
        assert json.load(fd) == {'pending': False, 'completed': True, 'error': False}
