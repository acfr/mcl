"""Interface specification for publishing and receiving data in MCL.

This module defines an interface for publishing and receiving data in MCL. This
is done by providing abstract objects for broadcasting and listening for
data. The interface defined by these objects helps insure new interface
implementations will integrate with MCL.

The following abstract objects are defined:

    - :class:`~.abstract.Connection`
    - :class:`.RawBroadcaster`
    - :class:`.RawListener`

For examples of how to use :mod:`.abstract` to integrate a new network
interface into MCL see :mod:`.network.udp`.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import abc
import sys
import keyword
import operator
import textwrap
import mcl.event.event


class _ConnectionMeta(type):
    """Meta-class for manufacturing network interface connection objects.

    The :class:`._ConnectionMeta` object is a meta-class designed to
    manufacture MCL network interface :class:`~.abstract.Connection`
    classes. Connection objects behave like namedtuples. The meta-class works
    by dynamically adding mandatory and optional attributes to a class
    definition at run time if and ONLY if the class inherits from
    :class:`~.abstract.Connection`.

    Classes that inherit from :class:`~.abstract.Connection` must implement the
    attributes `mandatory`, `broadcaster` and `listener` where:

        - `mandatory` is a list of strings defining the names of mandatory
          message attributes that must be present when instances of the new
          :class:`.Message` objects are created. During instantiation the input
          list *args is mapped to the attributes defined by `mandatory`. If
          `mandatory` is not present, a TypeError will be raised.

        - `broadcaster` is a reference to the :class:`.RawBroadcaster` object
          associated with the :class:`~.abstract.Connection` object.

        - `listener` is a reference to the :class:`.RawListener` object
          associated with the :class:`~.abstract.Connection` object.

    Classes that inherit from :class:`~.abstract.Connection` can optionally
    implement the `optional` attribute where:

        - `optional` is a dictionary of optional connection parameters and
          their defaults. Keywords represent attribute names and the
          corresponding value represents the default value. During
          instantiation of the new Connection object, **kwargs is mapped to the
          attributes defined by `optional`. Note that `optional` is not
          required.

    These attributes are used to manufacture an object to contain the
    definition. See :class:`~.abstract.Connection` for implementation detail.

    Note that classes that do not inherit from :class:`~.abstract.Connection`
    will be left unmodified. These are the :class:`~.abstract.Connection`
    object and objects which sub-class a sub-class of
    :class:`~.abstract.Connection`.

    Raises:
        TypeError: If the parent class is a :class:`~.abstract.Connection`
            object or any of the mandatory or optional attributes are
            ill-specified.
        ValueError: If any of the mandatory or optional attribute names are
            ill-specified.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a network interface connection class.

        Manufacture a network interface class for objects inheriting from
        :class:`~.abstract.Connection`. This is done by searching the input
        dictionary `dct` for the keys `mandatory` and `optional` where:

            - `mandatory` is a list of strings defining the names of mandatory
              message attributes that must be present when instances of the new
              :class:`.Message` objects are created. During instantiation the
              input list *args is mapped to the attributes defined by
              `mandatory`. If `mandatory` is not present, a TypeError will be
              raised.

            - `broadcaster` is a reference to the :class:`.RawBroadcaster`
              object associated with the :class:`~.abstract.Connection`
              object.

            - `listener` is a reference to the :class:`.RawListener` object
              associated with the :class:`~.abstract.Connection` object.

            - `optional` is a dictionary of optional connection parameters and
              their defaults. Keywords represent attribute names and the
              corresponding value represents the default value. During
              instantiation of the new Connection object, the input dictionary
              **kwargs is mapped to the attributes defined by
              `optional`. `optional` is not required.

        A new connection class is manufactured using the definition specified
        by the attributes. Note that none of the attribute names can be set to
        `mandatory`, `broadcaster` or `listener`.

        Args:
          cls (class): is the class being instantiated.
          name (string): is the name of the new class.
          bases (tuple): base classes of the new class.
          dct (dict): dictionary mapping the class attribute names to objects.

        Returns:
            :class:`~.abstract.Connection`: sub-class of
                :class:`~.abstract.Connection` with attributes defined by the
                original `mandatory` and `optional` attributes.

        Raises:
            TypeError: If the mandatory or optional attributes are
                ill-specified.
            ValueError: If any of the mandatory or optional attribute names are
                ill-specified.

        """

        # NOTE: This code essentially manufactures a 'namedtuple' object using
        #       code adapted from the python library:
        #
        #           https://docs.python.org/2/library/collections.html#collections.namedtuple
        #
        #       This allows the attributes in the object to be immutable
        #       (read-only) one created. Note that all of the objects that are
        #       manufactured also inherit from the Connection() class.

        # Do not look for 'mandatory'/'optional' attributes in the Connection()
        # base class.
        if (name == 'Connection') and (bases == (tuple,)):
            return super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)

        #  Do not look for 'mandatory'/'optional' attributes in sub-classes of
        # the Connection() base class.
        elif bases != (Connection,):
            return super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)

        # Objects inheriting from Connection() are required to have a
        # 'mandatory' attribute. The 'optional' and 'docstring' are optional.
        mandatory = dct.get('mandatory', {})
        broadcaster = dct.get('broadcaster', None)
        listener = dct.get('listener', None)
        optional = dct.get('optional', {})

        # Ensure 'mandatory' is a list or tuple of strings.
        if (((not isinstance(mandatory, (list, tuple))) or
             (not all(isinstance(item, basestring) for item in mandatory)))):
            msg = "'mandatory' must be a string or a list/tuple or strings."
            raise TypeError(msg)

        # Ensure 'broadcaster' is a RawBroadcaster() object.
        if not broadcaster or not issubclass(broadcaster, RawBroadcaster):
            msg = "'broadcaster' must reference a RawBroadcaster() sub-class."
            raise TypeError(msg)

        # Ensure 'listener' is a RawListener() object.
        if not listener or not issubclass(listener, RawListener):
            msg = "'listener' must reference a RawListener() sub-class."
            raise TypeError(msg)

        # Ensure 'optional' is a list or tuple.
        if not isinstance(optional, (dict,)):
            msg = "'optional' must be a dictionary."
            raise TypeError(msg)

        # Ensure all keys in 'optional' are a string.
        if not all(isinstance(key, basestring) for key in optional.keys()):
            msg = "All keys in 'optional' must be strings."
            raise TypeError(msg)

        # Add optional fields.
        attrs = tuple(list(mandatory) + list(optional.keys()))

        # Parse and validate the field names. Validation serves two purposes,
        # generating informative error messages and preventing template
        # injection attacks.
        for attr in (name,) + attrs:
            if not all(c.isalnum() or c == '_' for c in attr):
                msg = 'Type names and field names can only contain '
                msg += 'alphanumeric characters and underscores: %r'
                raise ValueError(msg % attr)

            if keyword.iskeyword(attr):
                msg = 'Type names and field names cannot be a keyword: %r'
                raise ValueError(msg % attr)

            if attr[0].isdigit():
                msg = 'Type names and field names cannot start with a number: '
                msg += '%r'
                raise ValueError(msg % attr)

        # Detect duplicate attribute names.
        invalid = ['_mandatory', '_optional', 'broadcaster', 'listener', ]
        seen_attr = set()
        for attr in attrs:
            if attr in invalid:
                msg = "Field names cannot be %r." % invalid
                raise ValueError(msg)
            if attr.startswith('_'):
                msg = 'Field names cannot start with an underscore: %r' % attr
                raise ValueError(msg)
            if attr in seen_attr:
                raise ValueError('Encountered duplicate field name: %r' % attr)
            seen_attr.add(attr)

        # Create 'prototype' for defining a new object.
        numfields = len(attrs)
        inputtxt = ', '.join(mandatory)
        if optional:
            for key, value in optional.iteritems():
                inputtxt += ", %s=%r" % (key, value)

        # Create strings for arguments and printing.
        argtxt = repr(attrs).replace("'", "")[1:-1]
        reprtxt = ', '.join('%s=%%r' % attr for attr in attrs)

        # Create mapping object (key-value pairs).
        dicttxt = ['%r: t[%d]' % (n, p) for p, n in enumerate(attrs)]
        dicttxt = ', '.join(dicttxt)

        def execute_template(template, key, namespace={}):

            template = textwrap.dedent(template)
            try:
                exec template in namespace
            except SyntaxError, e:
                raise SyntaxError(e.message + ':\n' + template)

            return namespace[key]

        __new__ = execute_template("""
        def __new__(cls, %s):
            return tuple.__new__(cls, (%s))
        """ % (inputtxt, argtxt), '__new__')

        _make = execute_template("""
        @classmethod
        def _make(cls, iterable, new=tuple.__new__, len=len):
            'Make a new %s object from a sequence or iterable'

            result = new(cls, iterable)
            if len(result) != %d:
                msg = 'Expected %d arguments, got %%d' %% len(result)
                raise TypeError(msg)
            return result
        """ % (name, numfields, numfields), '_make')

        __repr__ = execute_template("""
        def __repr__(self):
            return '%s(%s)' %% self
        """ % (name, reprtxt), '__repr__')

        to_dict = execute_template("""
        def to_dict(t):
            'Return a new dict which maps field names to their values'

            return {%s}
        """ % (dicttxt), 'to_dict')

        from_dict = execute_template("""
        @classmethod
        def from_dict(cls, dictionary):
            '''Make a new %s object from a dictionary

            If optional attributes are not specified, their default values are
            used.
            '''

            # Gather mandatory attributes.
            args = list()
            for attr in %r:
               if attr not in dictionary:
                    msg = "Expected the attribute: '%%s'." %% attr
                    raise AttributeError(msg)
               else:
                   args.append(dictionary[attr])

            # Gather optional attributes.
            for attr, value in cls._optional.iteritems():
               if attr in dictionary:
                   args.append(dictionary[attr])
               else:
                   args.append(value)

            return tuple.__new__(cls, tuple(args))
        """ % (name, mandatory), 'from_dict')

        _replace = execute_template("""
        def _replace(self, **kwds):
            'Return a new %s object replacing specified fields with new values'

            result = self._make(map(kwds.pop, %r, self))
            if kwds:
                msg = 'Got unexpected field names: %%r' %% kwds.keys()
                raise ValueError(msg)
            return result
        """ % (name, attrs), '_replace')

        def __getnewargs__(self):                            # pragma: no cover
            return tuple(self)

        # Remove specification.
        if 'mandatory' in dct: del dct['mandatory']
        if 'optional'  in dct: del dct['optional']

        # Add methods to class definition.
        dct['__slots__'] = ()
        dct['_mandatory'] = mandatory
        dct['_optional'] = optional
        dct['__new__'] = __new__
        dct['_make'] = _make
        dct['__repr__'] = __repr__
        dct['to_dict'] = to_dict
        dct['from_dict'] = from_dict
        dct['_replace'] = _replace
        dct['__getnewargs__'] = __getnewargs__

        # Add broadcaster and listener.
        dct['broadcaster'] = property(lambda self: broadcaster)
        dct['listener'] = property(lambda self: listener)

        # Add properties (read-only access).
        for i, attr in enumerate(attrs):
            dct[attr] = property(operator.itemgetter(i))

        # Create object.
        obj = super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)

        # For pickling to work, the __module__ variable needs to be set to the
        # frame where the named tuple is created.  Bypass this step in
        # enviroments where sys._getframe is not defined (Jython for example).
        if hasattr(sys, '_getframe'):
            obj.__module__ = sys._getframe(1).f_globals.get('__name__',
                                                            '__main__')

        return obj


class Connection(tuple):
    """Base class for MCL network interface connection objects.

    The :class:`~.abstract.Connection` object provides a base class for
    defining MCL network interface connection objects.  Classes that inherit
    from :class:`~.abstract.Connection` **must** implement the attributes
    `mandatory`, `broadcaster` and `listener` where:

        - `mandatory` is a list of strings defining the names of mandatory
          message attributes that must be present when instances of the new
          :class:`.Message` objects are created. During instantiation the input
          list \*args is mapped to the attributes defined by `mandatory`. If
          `mandatory` is not present, a :exc:`~python:exceptions.TypeError`
          will be raised.

        - `broadcaster` is a reference to the :class:`.RawBroadcaster` object
          associated with the :class:`~.abstract.Connection` object.

        - `listener` is a reference to the :class:`.RawListener` object
          associated with the :class:`~.abstract.Connection` object.

    Classes that inherit from :class:`~.abstract.Connection` can **optionally**
    implement the attribute `optional` where:

        - `optional` is a dictionary of optional connection parameters and
          their defaults. Keywords represent attribute names and the
          corresponding value represents the default value. During
          instantiation of the new Connection object, \**kwargs is mapped to
          the attributes defined by `optional`.

    These attributes form the definition of the network interface connection
    and allow :class:`~.abstract.Connection` to manufacture a connection class
    adhering to the specified definition. None of the attribute names can be
    set to `mandatory`, `broadcaster`, `listener` or `optional`.

    :class:`~.abstract.Connection` objects behave like
    :obj:`python:collections.namedtuple` objects. That is,
    :class:`~.abstract.Connection` objects have fields accessible by attribute
    lookup as well as being indexable and iterable. However, since
    :class:`~.abstract.Connection` objects are tuple-like, the data they
    contain is immutable after instantiation.

    Example usage:

    .. testcode::

        from mcl.network.abstract import Connection
        from mcl.network.abstract import RawListener
        from mcl.network.abstract import RawBroadcaster

        # Define new connection object WITH NO optional parameters (abstract
        # RawBroadcaster/Listener used for illustration).
        class ExampleConnection(Connection):
            mandatory = ('A',)
            broadcaster = RawBroadcaster
            listener = RawListener

        # Instantiate connection object.
        example = ExampleConnection('A')
        print example

        # Define new connection object WITH optional parameters.
        class ExampleConnection(Connection):
            mandatory = ('A',)
            optional  = {'B': 1, 'C': 2, 'D': 3}
            broadcaster = RawBroadcaster
            listener = RawListener

        # Instantiate connection object.
        example = ExampleConnection('A', D=5)
        print example

    .. testoutput::
       :hide:

       ExampleConnection(A='A')
       ExampleConnection(A='A', C=2, B=1, D=5)

    Raises:
        TypeError: If the mandatory or optional attributes are ill-specified.
        ValueError: If any of the mandatory or optional attribute names are
            ill-specified.

    """
    __metaclass__ = _ConnectionMeta


class RawBroadcaster(object):
    """Abstract base class for sending data over a network interface.

    The :class:`.RawBroadcaster` is an abstract base class designed to provide
    a template for objects in the MCL ecosystem which broadcast data over a
    network interface. Broadcasters inheriting from this template are likely to
    integrate safely with the MCL system.

    Args:
        connection (:class:`~.abstract.Connection`): Connection object.
        topic (str): Default topic associated with the network interface.

    Attributes:
        connection (:class:`~.abstract.Connection`): Connection object.
        topic (str): Default topic associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
            open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts issued.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    def __init__(self, connection, topic=None):
        """Document the __init__ method at the class level."""

        # Ensure the connection object is properly specified.
        if not isinstance(connection, Connection):
            msg = "The argument 'connection' must be an instance of a "
            msg += "Connection() subclass."
            raise TypeError(msg)

        # Broadcasters can only store ONE default topic. Enforce this behaviour
        # by only accepting a string.
        if topic is not None and not isinstance(topic, basestring):
            raise TypeError("The argument 'topic' must be None or a string.")

        # Save connection parameters.
        self.__connection = connection
        self.__topic = topic

    @property
    def connection(self):
        return self.__connection

    @property
    def topic(self):
        return self.__topic

    @abc.abstractproperty
    def is_open(self):
        pass                                                 # pragma: no cover

    @abc.abstractmethod
    def _open(self):
        """Virtual: Open connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                opened. If the network interface was already opened, the
                request is ignored and the method returns :data:`False`.

        """
        pass                                                 # pragma: no cover

    @abc.abstractmethod
    def publish(self, data, topic=None):
        """Virtual: Send data over network interface.

        Args:
            data (obj): Serialisable object to publish over the network
                interface.
            topic (str): Topic associated with published data. This option will
                temporarily override the topic specified during instantiation.

        """

        # Ensure topic is a string..
        if topic is not None and not isinstance(topic, basestring):
            raise TypeError("The argument 'topic' must be None or a string.")

    @abc.abstractmethod
    def close(self):
        """Virtual: Close connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                closed. If the network interface was already closed, the
                request is ignored and the method returns :data:`False`.

        """
        pass                                                 # pragma: no cover


class RawListener(mcl.event.event.Event):
    """Abstract base class for receiving data over a network interface.

    The :class:`.RawListener` is an abstract base class designed to provide a
    template for objects in the MCL ecosystem which listen for data over a
    network interface. Listeners inheriting from this template are likely to
    integrate safely with the MCL system.

    Network data is made available to subscribers by issuing callbacks, when
    data arrives, in the following format::

        {'topic': str,
         'payload': obj()}

    where:

        - **<topic>** is a string containing the topic associated with the
          received data.

        - **<payload>** is the received (serialisable) data.

    .. note::

        :class:`.RawListener` implements the event-based programming paradigm
        by inheriting from :class:`.Event`. Data can be issued to callback
        functions by calling the RawListener.__trigger__ method. This method
        has been removed from the public API to prevent *users* from calling
        the method. In concrete implementations of the :class:`.RawListener,
        *developers* can call the '__trigger__' method in I/O loops when
        network data is available.

    Args:
        connection (:class:`~.abstract.Connection`): Connection object.
        topics (str or list): Topics associated with the network interface
            represented as either a string or list of strings.

    Attributes:
        connection (:class:`~.abstract.Connection`): Connection object.
        topics (str or list): Topics associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
            open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts received.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    def __init__(self, connection, topics=None):
        """Document the __init__ method at the class level."""

        # Ensure the connection object is properly specified.
        if not isinstance(connection, Connection):
            msg = "The argument 'connection' must be a Connection() subclass."
            raise TypeError(msg)

        # Broadcasters can only store ONE default topic. Enforce this behaviour
        # by only accepting a string.
        if topics is not None and not isinstance(topics, basestring) and not \
           all(isinstance(item, basestring) for item in topics):
            msg = "The argument 'topics' must be None, a string or a list of "
            msg += "string."
            raise TypeError(msg)

        # Save connection parameters.
        self.__connection = connection
        self.__topics = topics

        # Initialise Event() object.
        super(RawListener, self).__init__()

    @property
    def connection(self):
        return self.__connection

    @property
    def topics(self):
        return self.__topics

    @abc.abstractproperty
    def is_open(self):
        pass                                                 # pragma: no cover

    @abc.abstractmethod
    def _open(self):
        """Virtual: Open connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                opened. If the network interface was already opened, the
                request is ignored and the method returns :data:`False`.

        """
        pass                                                 # pragma: no cover

    @abc.abstractmethod
    def close(self):
        """Virtual: Close connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                closed. If the network interface was already closed, the
                request is ignored and the method returns :data:`False`.

        """
        pass                                                 # pragma: no cover
