import plyvel
import time
import json
import os
import logging
import traceback

_START_FILENAME = 'start_time'
_END_FILENAME = 'end_time'
_STATUS_FILENAME = 'status'
_ERR_FILENAME = 'error.log'
_RESULT_FILENAME = 'result.json'
_STORAGE_DIRNAME = 'storage'
_LOG_FILENAME = 'run.log'


class Collection:

    def __init__(self, proj_dir, name, func):
        self.func = func
        self.name = name
        self.basedir = os.path.join(proj_dir, name)
        os.makedirs(self.basedir, exist_ok=True)
        self.db_status_path = os.path.join(self.basedir, '.status')
        self.queue_dir = os.path.join(self.basedir, '.updates')
        os.makedirs(self.queue_dir, exist_ok=True)

    def update_db(self):
        """
        Update the status levelDB for each resource in the update queue directory.
        """
        db = plyvel.DB(self.db_status_path, create_if_missing=True)
        try:
            for ident in os.listdir(self.queue_dir):
                status_path = os.path.join(self.basedir, ident, _STATUS_FILENAME)
                if os.path.exists(status_path):
                    with open(status_path) as fd:
                        status = fd.read()
                else:
                    status = 'unavailable'
                db.put(ident.encode(), status.encode())
                queue_file_path = os.path.join(self.queue_dir, ident)
                os.remove(queue_file_path)
        finally:
            db.close()


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

    def status(self, coll_name, resource_id):
        """
        Fetch status for a single resource
        """
        resource_id = str(resource_id)
        self._validate_resource_id(coll_name, resource_id)
        coll = self.collections[coll_name]
        coll.update_db()
        db = plyvel.DB(coll.db_status_path, create_if_missing=True)
        try:
            status = db.get(resource_id.encode())
        finally:
            db.close()
        if status:
            return status.decode()
        else:
            return 'unavailable'

    def stats(self, coll_name=None):
        if coll_name:
            # Get the total status counts for a single collection
            return self._coll_stats(coll_name)
        # Get totals for every collection
        counts = {}  # type: dict
        for coll_name in self.collections:
            counts[coll_name] = self._coll_stats(coll_name)
        return counts

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

    def _coll_stats(self, coll_name):
        """
        Fetch stats for a whole collection
        """
        self._validate_coll_name(coll_name)
        coll = self.collections[coll_name]
        coll.update_db()
        ret = {
            'complete': 0,
            'error': 0,
            'pending': 0,
            'unavailable': 0,
            'total': 0
        }
        keys = set(ret.keys())
        db = plyvel.DB(coll.db_status_path, create_if_missing=True)
        try:
            for key, val in db:
                val_str = val.decode()
                if val_str in keys:
                    ret[val_str] += 1
                else:
                    ret['unavailable'] += 1
                ret['total'] += 1
        finally:
            db.close()
        return {'counts': ret}

    def find_by_status(self, coll_name, status='complete'):
        """
        Return a list of resource ids for a collection based on their current status
        """
        self._validate_coll_name(coll_name)
        coll = self.collections[coll_name]
        coll.update_db()
        status_bin = status.encode()
        ids = []
        db = plyvel.DB(coll.db_status_path, create_if_missing=True)
        try:
            for key, value in db:
                if value == status_bin:
                    ids.append(key.decode())
        finally:
            db.close()
        return ids

    def fetch(self, coll_name, ident, args=None, recompute=False):
        """
        Compute a new entry for a resource, or fetch the precomputed entry.
        """
        self._validate_coll_name(coll_name)
        # Return value
        ident = str(ident)
        coll = self.collections[coll_name]
        coll.update_db()
        res = Resource(coll, ident)
        if not recompute and res.status == 'complete':
            return res
        if args is None:
            args = {}
        ctx = Context(coll_name, res.paths['base'])
        # Submit the job
        print(f'Computing resource "{ident}" in "{coll_name}"')
        return res.compute(args, ctx)


class Resource:

    def __init__(self, coll, ident):
        self.coll = coll
        self.ident = ident
        basedir = os.path.join(coll.basedir, ident)
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
        # Initialize some basic directories, if absent
        os.makedirs(basedir, exist_ok=True)
        os.makedirs(self.paths['storage'], exist_ok=True)
        # Load the resource's status
        if os.path.exists(self.paths['status']):
            with open(self.paths['status']) as fd:
                self.status = fd.read()
        else:
            self.status = 'unavailable'
        # Load the result JSON
        self.result = None
        if self.status == 'complete' and os.path.exists(self.paths['result']):
            with open(self.paths['result']) as fd:
                self.result = json.load(fd)
        # Load start and end times
        self.start_time = _read_time(self.paths['start_time'])
        self.end_time = _read_time(self.paths['end_time'])

    def _set_status(self, status):
        """Write out status to a file and place this resource's in the collection's queue to update."""
        with open(self.paths['status'], 'w') as fd:
            fd.write(status)
        self.status = status
        # Touch a file with our identifier as the name in the collection's
        # queue directory. When we do project.status(), this resource will first
        # get updated in the collection's leveldb.
        queue_path = os.path.join(self.coll.queue_dir, self.ident)
        _touch(queue_path)

    def compute(self, args, ctx):
        """
        Run the function to compute a resource, handling and saving errors.
        """
        # Clear out resource files
        to_overwrite = [_RESULT_FILENAME, _ERR_FILENAME, _LOG_FILENAME]
        for fn in to_overwrite:
            _touch(os.path.join(self.paths['base'], fn), overwrite=True)
        # Write out status
        self._set_status('pending')
        # Write out start and end time
        self.start_time = _write_time(self.paths['start_time'], ts=_time())
        self.end_time = _write_time(self.paths['end_time'], ts=None)
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
            self._set_status('error')
            return self
        finally:
            self.end_time = _write_time(self.paths['end_time'], ts=_time())
        self._set_status('complete')
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
    with open(path, 'w') as fd:
        fd.write(str(ts))
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
