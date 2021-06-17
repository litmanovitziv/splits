import os


def path_with_version(basepath, seqnum, suffix):
    return os.path.join(basepath, '%06d%s' % (seqnum, suffix))


def path_with_fillers(basepath, suffix, *args, seqnum=-1, sep='_'):
    file_labels = [('%06d' % seqnum) if seqnum > 0 else ''] + list(args)
    file_name = sep.join(filter(None, file_labels))
    return os.path.join(basepath, '%s%s' % (file_name, suffix))
