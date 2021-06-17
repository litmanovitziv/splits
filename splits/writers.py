import functools
import os
import math

from splits.util import path_with_version, path_with_fillers


class SplitWriter(object):
    def __init__(self, basepath,
                 suffix='',
                 lines_per_file=math.inf,
                 fileClass=open,
                 fileArgs={'mode': 'wb'}):
        # functools.update_wrapper(self, func)
        # self.func = func

        self.suffix = suffix
        self.basepath = basepath
        self.lines_per_file = lines_per_file
        self.fileClass = fileClass
        self.fileArgs = fileArgs
        self._labels = []
        self._seq_num = 0
        self._line_num = 0
        self._file_line_num = 0
        self._is_create = False
        self._written_file_paths = []
        self._current_file = self._create_file()

    # def __call__(self, *args, **kwargs):
    #     return self.func(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def labels(self):
        return self._labels

    @labels.setter
    def labels(self, input_labels):
        self._labels = input_labels
        self._is_create = True

    @staticmethod
    def writer_decorator(*args, **kwargs):
        def _writer_decorator(func):
            return SplitWriter(func, *args, **kwargs)

        return _writer_decorator

    def write(self, data):
        if not isinstance(data, bytes):
            data = data.encode('utf-8')
        cnt = data.count(b'\n')
        for index, line in enumerate(data.split(b'\n')):
            if index == cnt:
                self.write_line(line)
            else:
                self.write_line(line + b'\n')

    def writelines(self, lines):
        for line in lines:
            self.write_line(line)

    def write_line(self, line):
        f = self._get_current_file()
        if not isinstance(line, bytes):
            line = line.encode('utf-8')
        f.write(line)
        self._line_num += line.count(b'\n')
        self._file_line_num += line.count(b'\n')

    def close(self):
        if self._current_file:
            self._current_file.close()

        path = self.basepath[:-1] if self.basepath.endswith('/') else self.basepath
        path += '.manifest'

        f = self.fileClass(path, **self.fileArgs)
        f.write(b''.join([x + b'\n' for x in self._written_file_paths]))
        f.close()

    def _get_current_file(self):
        if self._is_create or self._file_line_num >= self.lines_per_file:

            if self._current_file:
                self._current_file.close()

            self._current_file = self._create_file()

        return self._current_file

    def _create_file(self):
        self._seq_num += 1
        self._file_line_num = 0

        path = path_with_fillers(self.basepath, self.suffix, *self._labels) if self._is_create \
            else path_with_version(self.basepath, self._seq_num, self.suffix)
        self._is_create = False

        self._written_file_paths.append(path.encode('utf-8'))
        return self.fileClass(path, **self.fileArgs)
