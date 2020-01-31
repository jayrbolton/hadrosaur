import traceback
import sys
import json
import os
from io import StringIO

_STATUS_FILENAME = 'status.json'
_ERR_FILENAME = 'error.log'
_RESULT_FILENAME = 'result.json'
_STDOUT_FILENAME = 'stdout.log'
_STDERR_FILENAME = 'stderr.log'


class Project:

    def __init__(self, basepath):
        if os.path.exists(basepath) and not os.path.isdir(basepath):
            raise RuntimeError(f"Project base path is not a directory: {basepath}")
        os.makedirs(basepath, exist_ok=True)
        self.basedir = basepath
        self.resources = {}  # type: dict

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
        os.makedirs(entry_path, exist_ok=True)
        # Check the current status of the resource
        status_path = os.path.join(entry_path, _STATUS_FILENAME)
        status = {'completed': False, 'pending': True, 'error': False}  # type: dict
        if os.path.exists(status_path):
            with open(status_path) as fd:
                status = json.load(fd)
            if status.get('pending'):
                raise RuntimeError("Resource is already being computed and is pending")
        result_path = os.path.join(entry_path, _RESULT_FILENAME)
        if not recompute and status.get('completed'):
            with open(result_path) as fd:
                print('Resource is already computed')
                return {'result': json.load(fd), 'status': status}
        print('Computing the requested resource')
        # Write status as pending
        status['pending'] = True
        with open(status_path, 'w') as fd:
            # TODO try/except
            json.dump(status, fd)
        # Save stdout and stderr from the function to a string
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        mystdout = StringIO()
        mystderr = StringIO()
        sys.stdout = mystdout
        sys.stderr = mystderr
        if args is None:
            args = {}
        try:
            result = func(ident, args, entry_path)
        except Exception:
            status['error'] = True
            err_str = traceback.format_exc()
            with open(os.path.join(entry_path, _ERR_FILENAME), 'a') as fd:
                fd.write(err_str)
            with open(status_path, 'w') as fd:
                status['error'] = True
                status['pending'] = False
                json.dump(status, fd)
            return {'result': None, 'status': status}
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            with open(os.path.join(entry_path, 'stdout.log'), 'w') as fd:
                fd.write(mystdout.getvalue())
            with open(os.path.join(entry_path, 'stderr.log'), 'w') as fd:
                fd.write(mystderr.getvalue())
        with open(result_path, 'w') as fd:
            # TODO try/except
            json.dump(result, fd)
        status['completed'] = True
        status['error'] = False
        status['pending'] = False
        # Write to status file
        with open(status_path, 'w') as fd:
            # TODO try/except
            json.dump(status, fd)
        return {'result': result, 'status': status}
