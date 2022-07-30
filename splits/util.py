import os


def path_with_version(basepath, seqnum, suffix):
    return os.path.join(basepath, '%06d%s' % (seqnum, suffix))


def path_with_fillers(basepath, suffix, *args, seqnum=-1, sep='_'):
    file_labels = list(filter(None, list(args))) or [('%06d' % seqnum) if seqnum > 0 else '']
    file_name = sep.join(file_labels)
    return os.path.join(basepath, '%s%s' % (file_name, suffix))
