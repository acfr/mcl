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
import traceback
from mcl import Publisher
from mcl.message.messages import Message
from mcl.message import get_message_object

from abc import abstractmethod
from abc import abstractproperty

HEADER_DELIMITER = ','
HEADER_FORMAT = HEADER_DELIMITER.join(['%i', '%s', '%i', '%i'])
HEADER_FORMAT += HEADER_DELIMITER


def _abstract_error(cls):
    """Function for throwing a NotImplementedError in virtual methods.

    The message assigned to the :exc:`NotImplementedError` is:

        The method '<fcn>' in '<cls>' is abstract. Derived classes must
        override this method.

    where:

        - <fcn> is the name of the abstract method
        - <cls> is the name of the abstract class

    """

    cls = cls.__class__.__name__
    fcn = traceback.extract_stack(None, 2)[0][2]
    message = "The method '%s' in '%s' is abstract. " % (fcn, cls)
    message += "Derived classes must override this method."

    return NotImplementedError(message)


class Connection(object):
    """Object for encapsulating connection parameters of network interfaces.

    The :py:class:`.Connection` object is an abstract specification designed to
    provide separation between pyITS network interfaces and the parameters used
    to create them. The interface provided by :py:class:`.Connection` ensures
    child objects will be compatible with pyITS. :py:class:`.Connection` also
    provides some boiler-plate checking form the attributes ``url``, ``topics``
    and ``message`` to ensure they are set correctly.

    Args:
        url (str): Uniform resource locator used to identify the network
                   interface.
        topics (str): Topics associated with the network interface.
        message (:py:class:`.Message`): pyITS message object associated with
                                        the network interface.

    Attributes:
        url (str): Uniform resource locator used to identify the network
                   interface. The `url` attribute must be a non-empty string.
        topics (str): Topics associated with the network interface. The
                      `topics` attribute can be :data:`None`, a single string
                      or a list of strings.
        message (:py:class:`.Message`): pyITS message object associated with
                                        the network interface.

    """

    # Ensure abstract methods are redefined in child classes.
    __metaclass__ = abc.ABCMeta

    # Character used to delimit topics in a string of topics for example:
    #
    #     topics = 'cat, dog, rat'
    #
    TOPIC_DELIMITER = ','

    def __init__(self, url, topics=None, message=None):
        """Document the __init__ method at the class level."""

        self.__url = url
        self.__topics = topics
        self.__message = message

    @property
    def url(self):
        return self.__url

    @url.setter
    def url(self, url):

        # Uniform resource identifier cannot be empty.
        if not url:
            raise TypeError('URL cannot be empty. Nominate a unique string.')

        # Throw error if object is not a string.
        elif not isinstance(url, basestring):
            raise TypeError('URL must be a string.')

        self.__url = url

    @property
    def topics(self):
        return self.__topics

    @topics.setter
    def topics(self, topics):

        # Permit empty string or None.
        if topics is '' or topics is None:
            self.__topics = None
            return None

        # If topics is a string, insert the string into a list to be validated
        # by the next logic block.
        if isinstance(topics, basestring):
            topics = [topics, ]

        # Validate list.
        if hasattr(topics, '__iter__'):
            for topic in topics:

                # Elements in list must be non-empty strings.
                if not isinstance(topic, basestring):
                    raise TypeError('Topic must be a string.')

                # Strings in the list must not contain the delimiter character.
                elif self.TOPIC_DELIMITER in topic:
                    msg = "The input topic '%s' cannot contain the '%s' "
                    msg += "character."
                    raise ValueError(msg % (topic, self.TOPIC_DELIMITER))

            # Single topics can be stored as a string.
            if len(topics) == 1:
                self.__topics = topics[0]

            # List of valid strings are stored as a list.
            else:
                self.__topics = topics

        # Input is the incorrect type.
        else:
            msg = 'Topics must be None, a string or list of strings.'
            raise TypeError(msg)

    @property
    def message(self):
        return self.__message

    @message.setter
    def message(self, message):

        # Parse messages.
        if message:

            # Message has been specified as a string.
            if isinstance(message, basestring):
                try:
                    message = get_message_object(message)
                except:
                    msg = "'%s' is not a valid pyITS message type."
                    raise TypeError(msg % message)

            # Message has been specified as a non-string object.
            else:
                msg = 'Expected %r, received %r' % (Message, message)
                try:
                    if not issubclass(message, Message):
                        raise TypeError(msg)
                except:
                    raise TypeError(msg)

            self.__message = message

        # Allow empty/null message objects.
        else:
            self.__message = None

    @abstractmethod
    def __str__(self):
        raise _abstract_error(self)

    @classmethod
    @abstractmethod
    def from_string(cls, string):
        """Create and configure a connection object from a string.

        Child objects must override this classmethod and implement a parsing
        function to convert a string into a connection object. This will allow
        :py:class:`.RawBroadcaster` and :py:class:`.RawListener` objects to be
        created from a configuration file.

        Args:
            string (str): string specifying the connection configuration.

        Returns:
            :py:class:`.Connection`: Configured :py:class:`.Connection` object.

        """
        raise _abstract_error(cls)


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
        raise _abstract_error(self)

    @abstractproperty
    def topic(self):
        raise _abstract_error(self)

    @abstractproperty
    def is_open(self):
        raise _abstract_error(self)

    @abstractproperty
    def counter(self):
        raise _abstract_error(self)

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
        raise _abstract_error(self)

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
        raise _abstract_error(self)

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
        raise _abstract_error(self)

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
        raise _abstract_error(self)

    @abstractproperty
    def topics(self):
        raise _abstract_error(self)

    @abstractproperty
    def is_open(self):
        raise _abstract_error(self)

    @abstractproperty
    def counter(self):
        raise _abstract_error(self)

    @abstractmethod
    def _open(self):
        """Virtual: Open connection to network interface.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)

    @abstractmethod
    def close(self):
        """Virtual: Close connection to network interface.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)

    @abstractmethod
    def from_connection(cls, connection):
        raise _abstract_error(cls)
