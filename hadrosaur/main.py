import signal
import time
import traceback
import json
import os
import logging

_STATUS_FILENAME = 'status.json'
_ERR_FILENAME = 'error.log'
_RESULT_FILENAME = 'result.json'
_STORAGE_DIRNAME = 'storage'
_LOG_FILENAME = 'run.log'

# TODO log colors
# TODO hard rest on recompute (delete everything first)


class Project:

    def __init__(self, basepath):
        if os.path.exists(basepath) and not os.path.isdir(basepath):
            raise RuntimeError(f"Project base path is not a directory: {basepath}")
        os.makedirs(basepath, exist_ok=True)
        self.basedir = basepath
        self.resources = {}  # type: dict
        self.logger = logging.getLogger(basepath)

    def resource(self, name):
        """
        Define a new resource by name and function
        """
        if name in self.resources:
            raise RuntimeError(f"Resource name has already been used: '{name}'")
        res_path = os.path.join(self.basedir, name)
        os.makedirs(res_path, exist_ok=True)

        def wrapper(func):
            self.resources[name] = {'func': func, 'dir': res_path}
            return func
        return wrapper

    def fetch(self, resource_name, ident, args=None, recompute=False):
        """
        Compute a new entry for a resource, or fetch the precomputed entry.
        """
        if resource_name not in self.resources:
            raise RuntimeError(f"No such resource: {resource_name}")
        ident = str(ident)
        res = self.resources[resource_name]
        func = res['func']
        dirpath = res['dir']
        entry_path = os.path.join(dirpath, ident)
        paths = {
            'base': entry_path,
            'error': os.path.join(entry_path, _ERR_FILENAME),
            'log': os.path.join(entry_path, _LOG_FILENAME),
            'status': os.path.join(entry_path, _STATUS_FILENAME),
            'result': os.path.join(entry_path, _RESULT_FILENAME),
            'storage': os.path.join(entry_path, _STORAGE_DIRNAME),
        }
        os.makedirs(entry_path, exist_ok=True)
        os.makedirs(paths['storage'], exist_ok=True)
        # Check the current status of the resource
        status = {'completed': False, 'pending': True, 'error': False}  # type: dict
        if os.path.exists(paths['status']):
            with open(paths['status']) as fd:
                status = json.load(fd)
            if status.get('pending'):
                raise RuntimeError("Resource is already being computed and is pending")
        # Check if it's already computed
        result_path = os.path.join(entry_path, _RESULT_FILENAME)
        if not recompute and status.get('completed'):
            with open(result_path) as fd:
                print(f'Resource "{ident}" in "{resource_name}" is already computed')
                return {'result': json.load(fd), 'status': status, 'paths': paths}

        # Handle control-C interrupts
        def exit_gracefully(signum, frame):
            print('Cleaning up job')
            status['pending'] = False
            status['completed'] = False
            status['error'] = False
            status['end_time'] = int(time.time() * 1000)
            with open(paths['status'], 'w') as fd:
                json.dump(status, fd)
        signal.signal(signal.SIGINT, exit_gracefully)

        # Compute the resource
        print('Computing resource "{ident}" in "{resource_name}"')
        # Write status as pending
        status['pending'] = True
        status['error'] = False
        status['start_time'] = int(time.time() * 1000)
        status['end_time'] = None
        with open(paths['status'], 'w') as fd:
            json.dump(status, fd)
        if args is None:
            args = {}
        # Clear the error file
        with open(paths['error'], 'w') as fd:
            fd.write('')
        ctx = Context(resource_name, entry_path)
        try:
            result = func(ident, args, ctx)
        except Exception:
            status['error'] = True
            err_str = traceback.format_exc()
            with open(os.path.join(entry_path, _ERR_FILENAME), 'a') as fd:
                fd.write(err_str)
            with open(paths['status'], 'w') as fd:
                status['error'] = True
                status['pending'] = False
                status['end_time'] = int(time.time() * 1000)
                json.dump(status, fd)
            return {'result': None, 'status': status, 'paths': paths}
        with open(result_path, 'w') as fd:
            json.dump(result, fd)
        status['completed'] = True
        status['error'] = False
        status['pending'] = False
        status['end_time'] = int(time.time() * 1000)
        # Write to status file
        with open(paths['status'], 'w') as fd:
            json.dump(status, fd)
        return {'result': result, 'status': status, 'paths': paths, 'paths': paths}


class Context:

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
