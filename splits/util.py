import os


def path_with_version(basepath, seqnum, suffix):
    return os.path.join(basepath, '%06d%s' % (seqnum, suffix))


def path_with_fillers(basepath, suffix, *args, sep='_'):
    file_name = sep.join(args)
    return os.path.join(basepath, '%s%s' % (file_name, suffix))
