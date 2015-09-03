"""Module specifying interface for publishing and receiving data in pyITS.

This module defines an interface for publishing and receiving data to network
interfaces in pyITS. This is done by providing abstract objects for
broadcasting and listening for data. The following abstract objects are defined
and ensure network interfaces can be integrated into pyITS:

    - :py:class:`.Connection`
    - :py:class:`.RawBroadcaster`
    - :py:class:`.RawListener`

For examples of how to use :py:mod:`.abstract` to integrate a new network
interface into pyITS see :py:mod:`.network.udp`.

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

from mcl import Publisher


class Connection(type):
    """Meta-class for manufacturing network interface connection objects.

    The :py:class:`.Connection` object is a meta-class designed to manufacture
    MCL network interface connection objects. The meta-class works by
    dynamically adding mandatory and optional parameters to a class definition
    at run time. This is done by searching for the '__mandatory__' and
    '__optional__' attributes where:

        - mandatory is a list of mandatory connection parameter names.
        - optional is a dictionary of optional connection parameters and their
          defaults.

    Example usage:

        # Define new connection object.
        class ExampleConnection(object):
            __metaclass__ = ConnectionMeta
            __mandatory__ = ('A', 'B')
            __optional__  = {'C': 1, 'D': 2}

        # Instantiate object.
        example = ExampleConnection('A', 'B', D=5)
        print example

    Raises:
        TypeError: If any of the input argument are invalid.

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

        # Access mandatory and optional parameters.
        MANDATORY = dct.get('__mandatory__', None)
        OPTIONAL = dct.get('__optional__', None)

        # Ensure 'mandatory' is a list or tuple.
        if ((not isinstance(MANDATORY, (list, tuple))) or
            (not all(isinstance(item, basestring) for item in MANDATORY))):
            msg = "'__mandatory__' must be a list or tuple or strings."
            raise TypeError(msg)

        # Ensure 'optional' is a dictionary or None.
        if not isinstance(OPTIONAL, (dict, None)):
            msg = "'__optional__' must be a dictionary."
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

        # Remove mandatory and optional fields from the class definition. They
        # are already embedded in the class definition via the __init__()
        # function.
        del dct['__mandatory__']
        del dct['__optional__']

        # Return class with factory methods.
        dct['__init__'] = __init__
        dct['__str__'] = __str__

        # Return factory class.
        return type.__new__(cls, name, bases, dct)


class RawBroadcaster(object):
    """Abstract object for sending data over a network interface.

    The :py:class:`.RawBroadcaster` is an abstract object designed to provide a
    template for objects in the pyITS ecosystem which broadcast data over a
    network interface. Broadcasters inheriting from this template are likely to
    integrate safely with the pyITS system.

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

    @abstractmethod
    def from_connection(cls, connection):
        raise _abstract_error(cls)


class RawListener(Publisher):
    """Abstract object for receiving data over a network interface.

    The :py:class:`.RawListener` is an abstract object designed to provide a
    template for objects in the pyITS ecosystem which listen for data over a
    network interface. Listeners inheriting from this template are likely to
    integrate safely with the pyITS system.

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

    @abstractmethod
    def from_connection(cls, connection):
        pass
