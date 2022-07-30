import functools
import os
import re
import logging

logger = logging.getLogger('reader')


class SplitReader(object):
    def __init__(self,
                 resource,
                 fileClass = open,
                 fileArgs = {'mode': 'rb'},
                 re_filter=r'^.*'):

            self._basepath = None
            self.fileClass = fileClass
            self.fileArgs = fileArgs
            self._regex_filter = re.compile(re_filter)

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

    def __call__(self, func):
        @functools.wraps(func)
        def _reader_wrapper(*args, **kwargs):
            return func(self, *args, **kwargs)

        return _reader_wrapper

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    @property
    def basepath(self):
        return self._basepath

    def next(self):
        line = self.readline()
        if line:
            return line
        raise StopIteration()

    def close(self):
        self._current_file.close()
        logger.info('closing file {}'.format(self._current_file.name))

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
                    logger.info('closing file {}'.format(self._current_file.name))
                else:
                    val += new_data

                if num > 0 and len(val) == num:
                    break
        except StopIteration:
            pass

        logger.debug(val)
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
                    logger.info('closing file {}'.format(self._current_file.name))
                else:
                    line += new_data

                if limit > 0 and len(line) == limit:
                    break
                elif line.endswith(b'\n'):
                    break
        except StopIteration:
            pass

        logger.debug(line)
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
            if not self._regex_filter.search(path): continue
            path_dir, path_file = os.path.split(path)
            self._basepath = path_dir if path_dir else None
            f = self.fileClass(path.strip(), **self.fileArgs)
            logger.info('opening file {}'.format(path))
            yield f
            f.close()

    def _get_files_tree(self, basepath, only_leaves=True):
        # root path, directories names, files' names
        for root, dirs, files in os.walk(basepath, topdown=False):
            if not only_leaves and not dirs: continue
            self._basepath = root
            for file in files:
                if not self._regex_filter.search(file): continue
                f = self.fileClass(os.path.join(self._basepath, file).strip(), **self.fileArgs)
                logger.info('opening file {}'.format(file))
                yield f
                f.close()
