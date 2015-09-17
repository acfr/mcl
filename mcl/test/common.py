"""Common tools for unit-testing.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import types


def attr_exists(dct, attrs):
    """Check object contains mandatory attributes."""
    for attr in attrs:
        if attr not in dct:
            msg = "The attribute '%s' is required." % str(attr)
            raise TypeError(msg)


def attr_issubclass(dct, key, obj, msg):
    """Check object attribute is a sub-class of a specific object."""
    if not issubclass(dct[key], obj):
        raise TypeError(msg)


def attr_isinstance(dct, key, obj, msg):
    """Check object attribute is an instance of a specific object."""
    if not isinstance(dct[key], obj):
        raise TypeError(msg)


def compile_docstring(base, name):
    """Rename dosctring of test-methods in base object."""

    # Iterate through items in the base-object.
    dct = dict()
    for item in dir(base):

        # Skip special attributes.
        if item.startswith('__'):
            continue

        # Inspect callable objects.
        if callable(getattr(base, item)):
            func = getattr(base, item)
            dct[item] = types.FunctionType(func.func_code,
                                           func.func_globals,
                                           item,
                                           func.func_defaults,
                                           func.func_closure)

            # Rename the doc-string of test methods in the base-object.
            if item.startswith('test_'):
                dct[item].__doc__ = dct[item].__doc__ % name

    return dct
