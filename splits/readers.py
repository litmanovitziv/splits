import functools
import os


class SplitReader(object):
    def __init__(self, func,
                 resource,
                 fileClass = open,
                 fileArgs = {'mode': 'rb'}):

            functools.update_wrapper(self, func)
            self.func = func

            self.fileClass = fileClass
            self.fileArgs = fileArgs

            if type(resource) == list:
                self.manifest = iter(self._get_files_list(resource))
            elif os.path.isdir(resource):
                self.manifest = iter(self._get_files_tree(resource))
            else:
                if not resource.endswith('.manifest'):
                    resource += '.manifest'

                with self.fileClass(resource, **self.fileArgs) as f:
                    # remove newlines from filenames
                    self.manifest = iter(self._get_files_list(f.readlines()))

            self._current_file = next(self.manifest)

    def __call__(self, *args, **kwargs):
        return self.func(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @staticmethod
    def reader_decorator(*args, **kwargs):
        def _reader_decorator(func):
            return SplitReader(func, *args, **kwargs)

        return _reader_decorator

    def next(self):
        line = self.readline()
        if line:
            return line
        raise StopIteration()

    def close(self):
        pass

    def read(self, num=None):
        val = b''

        if num is None:
            num = 0

        try:
            while True:
                if num > 0:
                    new_data = self._get_current_file().read(num - len(val))
                else:
                    new_data = self._get_current_file().read()

                if not new_data:
                    self._current_file.close()
                else:
                    val += new_data

                if num > 0 and len(val) == num:
                    break
        except StopIteration:
            pass

        return val

    def readline(self, limit=None):
        line = b''

        if limit is None:
            limit = 0

        try:
            while True:
                if limit > 0:
                    new_data = self._get_current_file().readline(
                        limit - len(line))
                else:
                    new_data = self._get_current_file().readline()

                if not new_data:
                    self._current_file.close()
                else:
                    line += new_data

                if limit > 0 and len(line) == limit:
                    break
                elif line.endswith(b'\n'):
                    break
        except StopIteration:
            pass

        return line

    def readlines(self, sizehint=None):
        all_lines = []
        line = self.readline()

        while line:
            all_lines.append(line)
            line = self.readline()

        return all_lines

    def _get_current_file(self):
        if self._current_file.closed:
            self._current_file = next(self.manifest)

        return self._current_file

    def _get_files_list(self, path_list):
        for path in path_list:
            f = self.fileClass(path.strip(), **self.fileArgs)
            yield f
            f.close()

    def _get_files_tree(self, basepath, only_leaves = True):
        # root path, directories names, files' names
        for root, dirs, files in os.walk(basepath, topdown=False):
            if not only_leaves and not dirs: continue
            for file in files:
                f = self.fileClass(os.path.join(root, file).strip(), **self.fileArgs)
                yield f
                f.close()
