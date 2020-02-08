import plyvel
import time
import json
import os
import logging
from enum import Enum
import threading
import traceback

_START_FILENAME = 'start_time'
_END_FILENAME = 'end_time'
_STATUS_FILENAME = 'status'
_ERR_FILENAME = 'error.log'
_RESULT_FILENAME = 'result.json'
_STORAGE_DIRNAME = 'storage'
_LOG_FILENAME = 'run.log'

# TODO log colors


class Status(Enum):
    """
    Valid resource status values
    """
    COMPLETE = b'complete'
    ERROR = b'error'
    PENDING = b'pending'


class Collection:

    def __init__(self, proj_dir, name, func):
        self.func = func
        self.name = name
        self.basedir = os.path.join(proj_dir, name)
        self.status_path = os.path.join(self.basedir, 'status.json')
        os.makedirs(self.basedir, exist_ok=True)
        status_path = os.path.join(self.basedir, 'status')
        self.db_status = plyvel.DB(status_path, create_if_missing=True)

    def exit(self):
        self.db_status.close()


class Project:

    def __init__(self, basepath):
        if os.path.exists(basepath) and not os.path.isdir(basepath):
            raise RuntimeError(f"Project base path is not a directory: {basepath}")
        os.makedirs(basepath, exist_ok=True)
        self.basedir = basepath
        self.collections = {}  # type: dict
        self.logger = logging.getLogger(basepath)

    def collection(self, name):
        """
        Define a new collection of resources by name and function
        """
        if name in self.collections:
            raise RuntimeError(f"Collection name has already been used: '{name}'")

        def wrapper(func):
            self.collections[name] = Collection(self.basedir, name, func)
            return func
        return wrapper

    def status(self, coll_name=None, resource_id=None):
        """
        Fetch some aggregated statistics about a collection.
        """
        if coll_name and resource_id:
            return self._resource_status(coll_name, resource_id)
        elif coll_name:
            return self._coll_status(coll_name)
        else:
            raise TypeError("Pass in a collection name or both a collection name and resource ID.")

    def _validate_coll_name(self, coll_name):
        """
        Make sure the collection exists for this project.
        """
        if coll_name not in self.collections:
            raise RuntimeError(f"Unknown collection '{coll_name}'")
        coll_path = os.path.join(self.basedir, coll_name)
        if not os.path.isdir(coll_path):
            raise RuntimeError(f"Collection directory `{coll_path}` is missing")

    def _validate_resource_id(self, coll_name, resource_id):
        """
        Make sure the collection and resource exists
        """
        self._validate_coll_name(coll_name)
        res_path = os.path.join(self.basedir, coll_name, resource_id)
        if not os.path.isdir(res_path):
            raise RuntimeError(f"Resource '{resource_id}' located at `{res_path}` does not exist.")

    def fetch_error(self, coll_name, resource_id):
        """
        Fetch the Python stack trace for a resource, if present
        """
        resource_id = str(resource_id)
        self._validate_resource_id(coll_name, resource_id)
        err_path = os.path.join(self.basedir, coll_name, resource_id, _ERR_FILENAME)
        if not os.path.isfile(err_path):
            return ''
        with open(err_path) as fd:
            return fd.read()

    def fetch_log(self, coll_name, resource_id):
        """
        Fetch the run log for a resource, if present
        """
        resource_id = str(resource_id)
        self._validate_resource_id(coll_name, resource_id)
        log_path = os.path.join(self.basedir, coll_name, resource_id, _LOG_FILENAME)
        if not os.path.isfile(log_path):
            return ''
        with open(log_path) as fd:
            return fd.read()

    def _resource_status(self, coll_name, resource_id):
        """
        Fetch stats for a single resource
        """
        resource_id = str(resource_id)
        self._validate_resource_id(coll_name, resource_id)
        res_path = os.path.join(self.basedir, coll_name, resource_id)
        status_path = os.path.join(res_path, _STATUS_FILENAME)
        with open(status_path) as fd:
            status = fd.read()
        return status

    def _coll_status(self, coll_name):
        """
        Fetch stats for a whole collection
        """
        self._validate_coll_name(coll_name)
        coll = self.collections[coll_name]
        return coll.status

    def find_by_status(self, coll_name, status='completed'):
        """
        Return a list of resource ids for a collection based on their current status
        """
        self._validate_coll_name(coll_name)
        coll = self.collections[coll_name]
        status_bin = status.encode()
        ids = []
        for key, value in coll.db_status:
            if value == status_bin:
                ids.append(key.decode())
        return ids

    def fetch(self, coll_name, ident, args=None, recompute=False, block=False):
        """
        Compute a new entry for a resource, or fetch the precomputed entry.
        """
        if coll_name not in self.collections:
            raise RuntimeError(f"No such collection: {coll_name}")
        self._validate_coll_name(coll_name)
        # Return value
        ident = str(ident)
        coll = self.collections[coll_name]
        res = Resource(coll, ident)
        if not recompute and res.status != 'pending':
            return res
        if args is None:
            args = {}
        ctx = Context(coll_name, res.paths['basedir'])
        # Submit the job
        print(f'Computing resource "{ident}" in "{coll_name}"')
        res.start_compute()
        if block:
            return res.compute(args, ctx)
        else:
            thread = threading.Thread(target=res.compute, args=(args, ctx), daemon=True)
            thread.start()
            return res


class Resource:

    def __init__(self, coll, ident):
        self.coll = coll
        self.ident = ident
        basedir = os.path.join(coll.basedir, ident)
        os.makedirs(basedir, exist_ok=True)
        self.status = coll.db_status.get(ident.encode()).decode()
        self.paths = {
            'base': basedir,
            'error': os.path.join(basedir, _ERR_FILENAME),
            'log': os.path.join(basedir, _LOG_FILENAME),
            'status': os.path.join(basedir, _STATUS_FILENAME),
            'start_time': os.path.join(basedir, _START_FILENAME),
            'end_time': os.path.join(basedir, _END_FILENAME),
            'result': os.path.join(basedir, _RESULT_FILENAME),
            'storage': os.path.join(basedir, _STORAGE_DIRNAME),
        }
        os.makedirs(self.paths['storage'], exist_ok=True)
        with open(self.paths['status']) as fd:
            status_file = fd.read()
        # Sync db status with file system status
        if status_file != self.status:
            self.status = status_file
            coll.db_status.set(self.ident.encode(), self.status.encode())
        # Load the result JSON
        self.result = None
        if os.path.exists(self.paths['result']):
            with open(self.paths['result']) as fd:
                self.result = json.load(self.paths['result'])
        # Load start and end times
        self.start_time = _read_time(self.paths['start_time'])
        self.end_time = _read_time(self.paths['end_time'])

    def start_compute(self):
        """
        Set various state for a resource in preparation of recomputing it.
        """
        # Clear out resource files
        to_overwrite = [_RESULT_FILENAME, _ERR_FILENAME, _LOG_FILENAME]
        for fn in to_overwrite:
            _touch(os.path.join(self.paths['base'], fn), overwrite=True)
        # Write out status
        self.set_status('pending')
        # Write out start and end time
        self.start_time = _write_time(self.paths['start_time'], ts=_time())
        self.end_time = _write_time(self.paths['end_time'], ts=None)

    def set_status(self, status):
        """Write out status to file and db."""
        with open(self.paths['status'], 'w') as fd:
            fd.write(status)
        self.coll.set(self.ident.encode(), status.encode())

    def compute(self, args, ctx):
        """
        Run the function to compute a resource, handling and saving errors.
        """
        func = self.coll.func
        try:
            self.result = func(self.ident, args, ctx)
        except Exception:
            # There was an error running the resource's function
            self.result = None
            format_exc = traceback.format_exc()
            traceback.print_exc()
            with open(self.paths['error'], 'a') as fd:
                fd.write(format_exc)
            self.end_time = _write_time(self.paths['end_time'], ts=_time())
            self.write_status('error')
        self.write_status('complete')
        _json_dump(self.result, self.paths['result'])
        return self


class Context:
    """
    This is an object that is passed as the last argument to every resource compute function.
    Supplies extra contextual data, if needed, for the function.
    """

    def __init__(self, coll_name, base_path):
        self.subdir = os.path.join(base_path, _STORAGE_DIRNAME)
        # Initialize the logger
        self.logger = logging.getLogger(coll_name)
        fmt = "%(asctime)s %(levelname)-8s %(message)s (%(filename)s:%(lineno)s)"
        time_fmt = "%Y-%m-%d %H:%M:%S"
        formatter = logging.Formatter(fmt, time_fmt)
        log_path = os.path.join(base_path, _LOG_FILENAME)
        fh = logging.FileHandler(log_path)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        ch = logging.StreamHandler()
        ch.setLevel(logging.ERROR)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.setLevel(logging.DEBUG)
        print(f'Logging to {log_path} -- {self.logger}')


# -- Utils --
# ===========

def _time():
    """Current time in ms."""
    return int(time.time() * 1000)


def _write_time(path, ts=None):
    """
    Write the current time in ms to the file at path.
    Returns `ts`
    """
    if not ts:
        ts = ''
    ts = str(ts)
    with open(path, 'w') as fd:
        fd.write(ts)
    return ts


def _touch(path, overwrite=False):
    """Write a blank file to path. Overwrites."""
    if overwrite or not os.path.exists(path):
        with open(path, 'w') as fd:
            fd.write('')


def _json_dump(obj, path):
    """Write json to path."""
    with open(path, 'w') as fd:
        json.dump(obj, fd)


def _read_time(path):
    """Read time from a path. Returns None if unreadable."""
    if not os.path.exists(path):
        return None
    with open(path) as fd:
        try:
            return int(fd.read())
        except ValueError:
            return None


def _get_path(obj, keys):
    """
    Fetch a path out of a dict or list. Returns none if the path does not exist.
    eg. _get_path({'x': {'y': 1}}, ('x', 'y')) => 1
    eg. _get_path({'x': {'y': 1}}, ('x', 'z', 'q')) => None
    """
    curr = obj
    for key in keys:
        try:
            curr = obj[key]
        except Exception:
            return None
    return curr
