"""Module specifying interface for publishing and receiving data in MCL.

This module defines an interface for publishing and receiving data to network
interfaces in MCL. This is done by providing abstract objects for
broadcasting and listening for data. The following abstract objects are defined
and ensure network interfaces can be integrated into MCL:

    - :py:class:`.Connection`
    - :py:class:`.RawBroadcaster`
    - :py:class:`.RawListener`

For examples of how to use :py:mod:`.abstract` to integrate a new network
interface into MLC see :py:mod:`.network.udp`.

.. note::

    The :py:mod:`.abstract` module provides a framework for adding new
    communication protocols. Originally RabbitMQ was supported. Due to lack of
    use, the RabbitMQ module was removed. As a result the only communication
    protocol supported is :py:mod:`.network.udp`. This makes the generic
    :py:mod:`.abstract` interface somewhat redundant. It has been left in the
    code-base for historic and future proofing purposes. Please refer to git
    hashes up to 41a051f6229be6b496842e3070abcd96311371a0 for code implementing
    RabbitMQ.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import abc
from abc import abstractmethod
from abc import abstractproperty
from mcl.event.event import Publisher


class _ConnectionMeta(type):
    """Meta-class for manufacturing network interface connection objects.

    The :py:class:`.ConnectionMeta` object is a meta-class designed to
    manufacture MCL network interface connection objects. The meta-class works
    by dynamically adding mandatory and optional parameters to a class
    definition at run time if and ONLY if the class inherits from
    :py:class:`.Connection`.

    Classes that inherit from :py:class:`.Connection` must implement the
    ``mandatory`` attribute which is a list of strings specifying the name of
    mandatory attributes. Classes that inherit from :py:class:`.Connection` can
    optionally implement the ``optional`` attribute which is a dictionary of
    optional connection parameters and their defaults. These attributes are
    used to manufacture an object to contain the definition. See
    :py:class:`.Connection` for implementation detail.

    Note that classes that do not inherit from :py:class:`.Connection` will be
    left unmodified. These are the :py:class:`.Connection` object and objects
    which sub-class a sub-class of :py:class:`.Connection`.

    Raises:
        TypeError: If the parent class is a :py:class:`.Connection` object and
            either ``mandatory`` or ``optional`` are ill-specified.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture network interface.

        Args:
          cls (class): is the class being instantiated.
          name (string): is the name of the new class.
          bases (tuple): base classes of the new class.
          dct (dict): dictionary mapping the class attribute names to objects.

        Raises:
            TypeError: If any of the input argument are invalid.

        """

        # Do not look for manditory/optional fields in the Connection class.
        if (name == 'Connection') and (bases == (object,)):
            return super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)

        # Do not look for manditory/optional fields in the Connection class.
        elif bases != (Connection,):
            return super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)

        # Objects inheriting from Connection will be required to have a
        # 'mandatory' attribute.
        MANDATORY = dct.get('mandatory', {})
        OPTIONAL = dct.get('optional', {})

        # Ensure 'mandatory' is a list or tuple of strings.
        if ((not isinstance(MANDATORY, (list, tuple))) or
            (not all(isinstance(item, basestring) for item in MANDATORY))):
            msg = "'mandatory' must be a list or tuple or strings."
            raise TypeError(msg)

        # Ensure 'optional' is a list or tuple.
        if not isinstance(OPTIONAL, (dict,)):
            msg = "'optional' must be a dictionary."
            raise TypeError(msg)

        # Initialisation method defined using closure.
        def __init__(self, *args, **kwargs):
            """Initialise factory interface connection object.

            Args:
                *args (list): mandatory connection parameters.
                **kwargs (dict): optional connection parameters.

            Raises:
                TypeError: If any of the input argument are invalid.

            """

            # Get class name.
            name = self.__class__.__name__

            # Ensure all mandatory arguments are present.
            if len(MANDATORY) != len(args):
                msg = '%s() expected %i arguments, got %i.' % \
                      (name, len(MANDATORY), len(args))
                raise TypeError(msg)

            # There are elements in the input optional parameters that are not
            # in the recognised optional parameters.
            invalid = set(kwargs.keys()) - set(OPTIONAL.keys())
            if invalid:
                msg = '%s() got unexpected keyword arguments: %s.' % \
                      (name, ', '.join(invalid))
                raise TypeError(msg)

            # Store mandatory and optional fields.
            self.__mandatory = MANDATORY
            self.__optional = OPTIONAL

            # Add mandatory parameters.
            for i, name in enumerate(self.__mandatory):
                setattr(self, name, args[i])

            # Add default optional parameters.
            for name, default in self.__optional.iteritems():
                setattr(self, name, default)

            # Set optional parameters.
            for name, value in kwargs.iteritems():
                setattr(self, name, value)

        # Printing method defined using closure.
        def __str__(self):
            """Pretty print the mandatory and optional parameters.

            Returns:
                str: A human readable string of the object mandatory and
                    optional parameters.

            """

            # Get name of mandatory items.
            mandatory = list()
            for name in self.__mandatory:
                string = '%s:' % name
                mandatory.append((string, name))

            # Get name of optional items and their defaults.
            optional = list()
            for name, default in self.__optional.iteritems():
                string = '%s (optional, default=%s):' % (name, str(default))
                optional.append((string, name))

            # Get length of longest line.
            parameters = mandatory + optional
            length = max([len(s) for s, n in parameters])

            # Create parameter display.
            lines = ['%s() parameters:' % self.__class__.__name__, ]
            for string, name in parameters:
                value = str(getattr(self, name))
                lines.append('    ' + string.ljust(length) + ' ' + value)

            return '\n'.join(lines)

        # Return factory class.
        dct = {'__init__': __init__, '__str__': __str__}
        return super(_ConnectionMeta, cls).__new__(cls, name, bases, dct)


class Connection(object):
    """Base class for MCL network interface connection objects.

    The :py:class:`.Connection` object provides a base class for defining MCL
    network interface connection objects. Objects inheriting from
    :py:class:`.Connection` must implement the attribute ``mandatory`` where
    ``mandatory`` is a list of strings defining the names of mandatory
    connection parameters. Objects inheriting from :py:class:`.Connection` can
    optionally implement the attribute ``optional`` where ``optional`` is a
    dictionary of optional connection parameters and their defaults. These
    attributes form the definition of the network interface connection and
    allow :py:class:`.Connection` to manufacture a connection object with these
    attributes.

    Example usage::

        # Define new connection object.
        class ExampleConnection(Connection):
            mandatory = ('A',)
            optional  = {'B': 1, 'C': 2, 'D': 3}

        # Instantiate object.
        example = ExampleConnection('A', D=5)
        print example.A
        print example

    Raises:
        TypeError: If any of the input argument are invalid.

    """
    __metaclass__ = _ConnectionMeta


class RawBroadcaster(object):
    """Abstract object for sending data over a network interface.

    The :py:class:`.RawBroadcaster` is an abstract object designed to provide a
    template for objects in the MCL ecosystem which broadcast data over a
    network interface. Broadcasters inheriting from this template are likely to
    integrate safely with the MCL system.

    Attributes:
        url (str): URL of the network interface.
        topic (str): Topic associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
                        open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts issued.

    """

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    @abstractproperty
    def url(self):
        pass

    @abstractproperty
    def topic(self):
        pass

    @abstractproperty
    def is_open(self):
        pass

    @abstractproperty
    def counter(self):
        pass

    @abstractmethod
    def _open(self):
        """Virtual: Open connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                           opened. If the network interface was already opened,
                           the request is ignored and the method returns
                           :data:`False`.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        pass

    @abstractmethod
    def publish(self, data, topic=''):
        """Virtual: Send data over network interface.

        Args:
            data (str): Array of characters to publish over the network
                        interface.
            topic (str): Broadcast message with an associated topic. This
                        option will temporarily override the topic specified
                        during instantiation.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        pass

    @abstractmethod
    def close(self):
        """Virtual: Close connection to network interface.

        Returns:
            :class:`bool`: Returns :data:`True` if the network interface was
                           closed. If the network interface was already closed,
                           the request is ignored and the method returns
                           :data:`False`.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        pass


class RawListener(Publisher):
    """Abstract object for receiving data over a network interface.

    The :py:class:`.RawListener` is an abstract object designed to provide a
    template for objects in the MCL ecosystem which listen for data over a
    network interface. Listeners inheriting from this template are likely to
    integrate safely with the MCL system.

    Attributes:
        url (str): URL of the network interface.
        topics (str): Topics associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
                        open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts received.

    """

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    @abstractproperty
    def url(self):
        pass

    @abstractproperty
    def topics(self):
        pass

    @abstractproperty
    def is_open(self):
        pass

    @abstractproperty
    def counter(self):
        pass

    @abstractmethod
    def _open(self):
        """Virtual: Open connection to network interface.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        pass

    @abstractmethod
    def close(self):
        """Virtual: Close connection to network interface.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        pass
