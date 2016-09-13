import importlib
import sys


def call_callable(name, *args, **kwargs):
    """call a callable object.

    See :func:`symbol_by_name`.

    """
    return symbol_by_name(name)(*args, **kwargs)


def symbol_by_name(name, aliases={}, imp=None, package=None, sep='.', default=None, **kwargs):
    """Get symbol by qualified name.

    The name should be the full dot-separated path to the class.
        modulename:ClassName

    Example:
        popcorn.apps.hub:Hub
                         ^- class name

    If `aliases` is provided, a dict containing short name/long name
    mappings, the name is looked up in the aliases first.

    Examples:
        >>> symbol_by_name('popcorn.apps.hub:Hub')
        <class 'popcorn.apps.hub.Hub'>

        >>> symbol_by_name('hub', {
        ...     'hub': 'popcorn.apps.hub:Hub'})
        <class 'popcorn.apps.hub.Hub'>

    This also support staticmethod in class.
        modulename:ClassName.method
    
    Example:
        popcorn.apps.hub:Hub.scan
                         ^- class name
    """
    if imp is None:
        imp = importlib.import_module

    if not isinstance(name, str):
        return name                                 # already a class

    name = aliases.get(name) or name
    sep = ':' if ':' in name else sep
    module_name, _, cls_name = name.rpartition(sep)
    if not module_name:
        cls_name, module_name = None, package if package else cls_name
    try:
        try:
            module = imp(module_name, package=package, **kwargs)
        except ValueError as exc:
            raise(ValueError, ValueError("Couldn't import {0!r}: {1}".format(name, exc)), sys.exc_info()[2])
        if cls_name:
            obj = module
            for obj_name in cls_name.split('.'):
                obj = getattr(obj, obj_name)
            return obj
        else:
            return module
        return getattr(module, cls_name) if cls_name else module
    except (ImportError, AttributeError):
        if default is None:
            raise
    return default
