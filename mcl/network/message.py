"""Module for publishing MCL messages over a network.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
from mcl.message.messages import Message


class MessageBroadcaster(object):
    """Send messages over a network interface.

    The :py:class:`.MessageBroadcaster` object is a factory which manufactures
    objects for broadcasting MCL :py:class:`.Message` objects over a
    network. The returned object overloads the
    :py:meth:`.RawBroadcaster.publish` of a :py:class:`.RawBroadcaster` object
    to serialise the contents of a :py:class:`.Message` before
    transmission. `Message pack <http://msgpack.org/index.html>`_ is used to
    serialise :py:class:`.Message` objects into byte string.

    For a list of available methods and attributes in the returned object, see
    :py:class:`.RawBroadcaster`.

    Args:
        message (:py:class:`.Message`): MCL message object.
        topic (str): Topic associated with the network interface.

    """

    def __new__(cls, message, topic=None):

        # Ensure 'message' is a Message() object.
        if not issubclass(message, Message):
            msg = "'message' must reference a Message() sub-class."
            raise TypeError(msg)

        # Use closure to define a sub-class of the RawBroadcaster specified in
        # the message connection. Once defined, return an instance of the new
        # MessageBroadcaster() sub-class.
        message_type = message

        class MessageBroadcaster(message.connection.broadcaster):
            """Send messages over a network interface.

            The :py:class:`.MessageBroadcaster` object provides an interface
            for broadcasting MCL :py:class:`.Message` objects over a
            network. Before transmission, :py:class:`.MessageBroadcaster`
            serialises the contents of the message into a byte string using
            Message pack.

            :py:class:`.MessageBroadcaster` establishes a network connection
            using the information contained within the input
            :py:class:`.Message` type.

            For a list of available methods and attributes, see
            :py:class:`.RawBroadcaster`.

            Args:
                message (:py:class:`.Message`): MCL message object.
                topic (str): Topic associated with the network interface.

            """

            def publish(self, message, topic=None):
                """Send an MCL message over the network.

                Args:
                    message (:py:class:`.Message`): MCL message object.
                    topic (str): Broadcast message with an associated
                                 topic. This option will temporarily override
                                 the topic specified during instantiation.

                Raises:
                    TypeError: If the input `topic` is not a string. Or the
                        input message type differs from the message specified
                        during instantiation.
                    ValueError: If the input `topic` contains the header
                                delimiter character.

                """

                # Ensure 'message' is a Message() object.
                if not isinstance(message, message_type):
                    error_msg = "'msg' must reference a %s() instance."
                    raise TypeError(error_msg % message_type.__name__)

                # Attempt to serialise input data.
                try:
                    packed_data = message.encode()
                except:
                    raise TypeError('Could not encode input object')

                # Publish serialised data.
                super(MessageBroadcaster, self).publish(packed_data,
                                                        topic=topic)

        return MessageBroadcaster(message.connection, topic=topic)


class MessageListener(object):
    """Receive messages over a network interface.

    The :py:class:`.MessageListener` object is a factory which manufactures
    objects for receiving MCL :py:class:`.Message` objects over a network. The
    returned object inherits from the :py:class:`.RawListener` class. When data
    is received, it is decoded into a :py:class:`.Message` object before an
    event is raised to forward the received data to subscribed
    callbacks. `Message pack <http://msgpack.org/index.html>`_ is used to
    decode the received data.

    For a list of available methods and attributes in the returned object, see
    :py:class:`.RawListener`.

    Args:
        message (:py:class:`.Message`): MCL message object.
        topics (str): List of strings containing topics
                      :py:class:`.MessageListener` will receive and process.

    """

    def __new__(cls, message, topics=None):

        # Ensure 'message' is a Message() object.
        if not issubclass(message, Message):
            msg = "'message' must reference a Message() sub-class."
            raise TypeError(msg)

        # Use closure to define a sub-class of the RawListener specified in
        # the message connection. Once defined, return an instance of the new
        # MessageListener() sub-class.
        message_type = message

        class MessageListener(message.connection.listener):
            """Receive messages over a network interface.

            The :py:class:`.MessageListener` object provides an interface for
            receiving MCL :py:class:`.Message` objects over a network. After
            receiving data, :py:class:`.MessageListener` decodes the contents
            data into a :py:class:`.Message` object using Message pack.

            :py:class:`.MessageListener` establishes a network connection
            using the information contained within the input
            :py:class:`.Message` type.

            For a list of available methods and attributes, see
            :py:class:`.RawListener`.

            Args:
                message (:py:class:`.Message`): MCL message object.
                topic (str): Topic associated with the network interface.

            """

            def __trigger__(self, packed_data):
                """Distribute MCL message to subscribed callbacks."""

                # Attempt to serialise input data.
                try:
                    msg = message_type(packed_data[2])
                    super(MessageListener, self).__trigger__(msg)
                except Exception as e:
                    print e.message

        return MessageListener(message.connection, topics=topics)
