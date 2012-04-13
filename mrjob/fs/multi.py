import logging


log = logging.getLogger('mrjob.fs.multi')


class MultiFilesystem(object):

    def __init__(self, *filesystems):
        super(MultiFilesystem, self).__init__()
        self.filesystems = filesystems

    def __getattr__(self, name):
        # Forward through to children for backward compatibility
        for fs in self.filesystems:
            if hasattr(fs, name):
                return getattr(fs, name)
        raise AttributeError(name)

    def _do_action(self, action, path, *args, **kwargs):
        """Call **action** on each filesystem object in turn. If one raises an
        :py:class:`IOError`, save the exception and try the rest. If none
        succeed, re-raise the first exception.
        """

        first_exception = None

        for fs in self.filesystems:
            if fs.can_handle_path(path):
                try:
                    return getattr(fs, action)(path, *args, **kwargs)
                except IOError, e:
                    if first_exception is None:
                        first_exception = e

        raise first_exception

    def du(self, path_glob):
        return self._do_action('du', path_glob)

    def ls(self, path_glob):
        return self._do_action('ls', path_glob)

    def _cat_file(self, path):
        for line in self._do_action('_cat_file', path):
            yield line

    def cat(self, path):
        for filename in self.ls(path):
            for line in self._cat_file(filename):
                yield line

    def mkdir(self, path):
        return self._do_action('mkdir', path)

    def path_exists(self, path_glob):
        return self._do_action('path_exists', path_glob)

    def path_join(self, dirname, filename):
        return self._do_action('path_Join', dirname, filename)

    def rm(self, path_glob):
        return self._do_action('rm', path_glob)

    def touchz(self, path):
        return self._do_action('touchz', path)

    def md5sum(self, path_glob):
        return self._do_action('md5sum', path_glob)
