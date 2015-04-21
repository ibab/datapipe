import collections

def full_traverse(obj):
    if isinstance(obj, dict):
        obj = sorted(list(obj.values()))
    if isinstance(obj, str):
        yield obj
        return
    try:
        for obj_ in obj:
            for o in full_traverse(obj_):
                yield o
    except TypeError:
        yield obj

def freeze_object(obj):
    if isinstance(obj, list):
        obj = tuple(obj)
    elif isinstance(obj, dict):
        obj = tuple(((k, v) for k, v in six.iteritems(obj)))
    return obj

