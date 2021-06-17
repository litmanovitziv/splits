import functools
import os
import math

from splits.util import path_with_version, path_with_fillers


class SplitWriter(object):
    def __init__(self, basepath = None,
                 suffix='',
                 header='',
                 bulks_per_file=math.inf,
                 lines_per_file=math.inf,
                 fileClass=open,
                 fileArgs={'mode': 'wb'}):
        # functools.update_wrapper(self, func)
        # self.func = func

        self.suffix = suffix
        self._basepath = basepath
        self.bulks_per_file = bulks_per_file
        self.lines_per_file = lines_per_file
        self.fileClass = fileClass
        self.fileArgs = fileArgs
        self._current_labels = []
        self._file_id = 0
        self._line_num = 0
        self._file_bulk_num = 0
        self._file_line_num = 0
        self._header = header
        self._written_file_paths = []
        self._current_file = None

    # def __call__(self, *args, **kwargs):
    #     return self.func(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def labels(self):
        return self._current_labels

    @property
    def basepath(self):
        return self._basepath

    @property
    def header(self):
        return self._header

    @labels.setter
    def labels(self, input_labels):
        self._current_labels = input_labels

        if self._current_file:
            self._current_file.close()

        self._current_file = self._create_file()
        if self._header.strip():
            self._current_file.write(self._header.encode('utf-8'))

    @basepath.setter
    def basepath(self, dir_path):
        self._basepath = dir_path
        self._current_labels = []
        if self._current_file:
            self._current_file.close()

    @header.setter
    def header(self, new_header):
        self._header = new_header

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
                self._write_line(line)
            else:
                self._write_line(line + b'\n')

    def writelines(self, lines):
        for line in lines:
            if not isinstance(line, bytes):
                line = line.encode('utf-8')
            self._write_line(line)
        if lines:
            self._file_bulk_num += 1

    def _write_line(self, line):
        f = self._get_current_file()
        f.write(line)
        self._line_num += line.count(b'\n')
        self._file_line_num += line.count(b'\n')

    def close(self):
        if self._current_file:
            self._current_file.close()

        path = self._basepath[:-1] if self._basepath.endswith('/') else self._basepath
        path += '.manifest'

        f = self.fileClass(path, **self.fileArgs)
        header = (','.join([''] * (len(self._current_labels)+1)) + 'path').encode('utf-8')
        f.write(b'\n'.join([x for x in [header] + self._written_file_paths]))
        f.close()

    def _get_current_file(self):
        if (self._file_bulk_num >= self.bulks_per_file or
                self._file_line_num >= self.lines_per_file):

            if self._current_file:
                self._current_file.close()

            self._current_file = self._create_file()
            if self._header.strip():
                self._current_file.write(self._header.encode('utf-8'))

        return self._current_file

    def _create_file(self):
        self._file_id += 1
        self._file_line_num = 0
        self._file_bulk_num = 0

        path = path_with_fillers(self._basepath, self.suffix, *(['%06d' % self._file_id] + self._current_labels))

        self._written_file_paths.append((','.join(['%06d' % self._file_id] + self._current_labels) + ',' + path).encode('utf-8'))
        return self.fileClass(path, **self.fileArgs)
