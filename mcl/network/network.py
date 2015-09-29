"""Module for publishing MCL messages over a network.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import time
import Queue
import threading
import multiprocessing
from threading import Thread
from multiprocessing import Process

import mcl.logging.sys
import mcl.message.messages
from mcl.event.event import Event
from mcl.message.messages import Message
from mcl.event.event import _HideTriggerMeta
from mcl.network.abstract import Connection as AbstractConnection

# Time to wait for threads and processes to start/stop. This parameter could be
# exposed to the user. Currently it is viewed as an unnecessary tuning
# parameter.
TIMEOUT = 10


def _set_process_name(name):
    """Function for setting the name of new processes."""

    # Set the name of a new process if 'setproctitle' exists.
    try:
        from setproctitle import getproctitle as getproctitle
        from setproctitle import setproctitle as setproctitle

        current_name = getproctitle()
        name = current_name + ' -> ' + name
        setproctitle(name)

    # If 'setproctitle' does not exist. Do nothing.
    except:
        pass


def RawBroadcaster(connection, topic=None):
    """Return an object for sending data over a network interface.

    Args:
        connection (:py:class:`.Connection`): Connection object.
        topic (str): Topic associated with the network interface.

    Attributes:
        connection (:py:class:`.Connection`): Connection object.
        topic (str): Topic associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
                        open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts issued.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    # Ensure the connection object is properly specified.
    if not isinstance(connection, AbstractConnection):
        msg = "The argument 'connection' must be an instance of a "
        msg += "Connection()."
        raise TypeError(msg)

    try:
        return connection.broadcaster(connection, topic=topic)
    except:
        raise


def RawListener(connection, topics=None):
    """Return an object for receiving data over a network interface.

    Args:
        connection (:py:class:`.Connection`): Connection object.
        topics (str): Topics associated with the network interface.

    Attributes:
        connection (:py:class:`.Connection`): Connection object.
        topics (str): Topics associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
                        open. Otherwise returns :data:`False`.
        counter (int): Number of broadcasts received.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    # Ensure the connection object is properly specified.
    if not isinstance(connection, AbstractConnection):
        msg = "The argument 'connection' must be an instance of a "
        msg += "Connection()."
        raise TypeError(msg)

    try:
        return connection.listener(connection, topics=topics)
    except:
        raise


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


class QueuedListener(Event):
    """Open a broadcast address and listen for data.

    The :py:class:`.QueuedListener` object subscribes to a network
    broadcast and issues publish events when data is received.

    The difference between this object and other network listeners is that
    network data is received on a separate process and written to a
    multi-processing queue. The intention is to use a light-weight process to
    achieve more accurate timing by avoiding the GIL. Resource allocation is
    left to the operating system.

    A summary of the :py:class:`.QueuedListener` object is shown
    below::

                 Data broadcast           Data republished
                 (over network)           (local callbacks)
                      |                            ^
         _____________|____________________________|______________
        |_____________|____________________________|______________|
        |             |                            |              |
        |             |      QueuedListener()      |              |
        |             |                            |              |
        |     ________V_________           ________|_________     |
        |    |__________________|         |__________________|    |
        |    | Process          |         | Thread           |    |
        |    |                  |         |                  |    |
        |    |     Add data     |         |    Read data     |    |
        |    |     to queue     |         |    from queue    |    |
        |    |__________________|         |__________________|    |
        |             |                            ^              |
        |             v                            |              |
        |             ------------------------------              |
        |             |   multiprocessing.Queue()  |              |
        |             ------------------------------              |
        |_________________________________________________________|

    If network data is handled immediately upon reception, long callbacks may
    cause data packets to be lost. By inserting data into a queue on a separate
    process, it is less likely data will be dropped. A separate thread can read
    buffered network data from the queue and issue lengthy callbacks with
    minimal impact to the reception process. If data is received faster than it
    can be processed the queue will grow.

    Data are published as a dictionary in the following format::

        {'transmissions': int(),
         'topic': str(),
         'payload': str()}

    where:

        - ``transmissions`` is an integer representing the total number of data
          packets sent at the origin.
        - ``topic`` is a string representing the topic associated with the
          current data packet. This can be used for filtering broadcasts.
        - ``payload`` contains the contents of the data transmission as a
          string.

    Args:
        connection (:py:class:`.Connection`): MCL connection object.

    """

    # Rename the 'trigger' method to '__trigger__' so it is more 'private'.
    __metaclass__ = _HideTriggerMeta

    def __init__(self, connection):
        """Document the __init__ method at the class level."""

        # Ensure 'connection' is a Connection() object.
        if not isinstance(connection, mcl.network.abstract.Connection):
            msg = "'connection' must reference a Connection() instance."
            raise TypeError(msg)
        self.__connection = connection

        # Initialise Event() object.
        super(QueuedListener, self).__init__()

        # Log initialisation.
        mcl.logging.sys.info(self, "%s - instanting", str(self.__connection))

        # Create objects for inter-process communication.
        self.__queue = None
        self.__timeout = 0.1

        # Asynchronous objects. The process is used to enqueue data and the
        # thread is used to dequeue data.
        self.__reader = None
        self.__reader_run_event = threading.Event()
        self.__writer = None
        self.__writer_run_event = multiprocessing.Event()
        self.__is_alive = False

        # Attempt to connect to network interface.
        try:
            success = self._open()
        except:
            success = False

        if not success:
            msg = "Could not connect to '%s'." % str(connection)
            raise IOError(msg)

    def is_alive(self):
        """Return whether the object is listening for broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if the object is listening for
                           broadcasts. Returns :data:`False` if the object is
                           NOT listening for broadcast.

        """

        return self.__is_alive

    # Note: This method is implemented as a private static method. It is has
    #       been implemented as a static method to reinforce the idea that
    #       operations in this method are performed on a separate process (new
    #       memory space) without explicit reference to the class. The method
    #       has been encapsulated in the class to reinforce the idea that is it
    #       functionality that is particular to the class.
    #
    @staticmethod
    def __enqueue(class_name, run_event, connection, queue):
        """Light weight service to write incoming data to a queue."""

        # Attempt to set process name.
        proc_name = 'listener %s'
        proc_name = proc_name % str(connection)
        _set_process_name(proc_name)

        # Log start of process activity.
        run_event.set()

        # Note: lexical closure is (ab)used to provide non-local access to the
        #       'name', 'connection' and the 'queue' object. This function will
        #       be executed asynchronously from the listener object where the
        #       queue object would normally be out of scope. Closure allows the
        #       queue to remain accessible.
        #
        def enqueue(data):
            """Write broadcast to queue when data is received."""

            try:
                # Write data to queue.
                #
                # Note: Objects enqueued by the same process will always be in
                #       the expected order with respect to each other.
                #
                queue.put(data)

            except:
                pass

        # Start listening for network broadcasts.
        listener = RawListener(connection)
        listener.subscribe(enqueue)

        # Wait for user to terminate listening service.
        while run_event.is_set():
            try:
                time.sleep(0.25)
            except KeyboardInterrupt:
                break
            except:
                raise

        # Stop listening for data.
        listener.close()

    def __dequeue(self):
        """Light weight service to read data from queue and issue callbacks."""

        # Log start of thread activity.
        self.__reader_run_event.set()

        # Read data from the queue and trigger an event.
        while self.__reader_run_event.is_set():
            try:
                data = self.__queue.get(timeout=self.__timeout)
                self.__trigger__(data)

            except Queue.Empty:
                pass

            except:
                raise

    def _open(self):
        """Open connection to queued listener and start publishing broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if a connection to the queued
                           listener is opened. If the queued listener is
                           already open, the request is ignored and the method
                           returns :data:`False`.

        """

        # Start publishing broadcast-events on a thread.
        if not self.is_alive():
            mcl.logging.sys.info(self,
                                 "%s - start request",
                                 str(self.__connection))

            # Reset asynchronous objects.
            self.__queue = multiprocessing.Queue()

            # Create THREAD for dequeueing and publishing data.
            self.__reader_run_event.clear()
            self.__reader = Thread(target=self.__dequeue)

            # Create PROCESS for enqueueing data.
            self.__writer_run_event.clear()
            self.__writer = Process(target=self.__enqueue,
                                    args=(self.__class__.__name__,
                                          self.__writer_run_event,
                                          self.__connection,
                                          self.__queue,))

            # Start asynchronous objects and wait for them to become alive.
            self.__writer.daemon = True
            self.__reader.daemon = True

            # Wait for queue READER to start.
            start_wait = time.time()
            self.__reader.start()
            while not self.__reader_run_event.is_set():
                if (time.time() - start_wait) > TIMEOUT:
                    msg = '%s - timed out waiting for thread to start.'
                    msg = msg % str(self.__connection)
                    mcl.logging.sys.exception(self, msg)
                    raise Exception(msg)
                else:
                    time.sleep(0.01)

            # Wait for queue WRITER to start.
            start_wait = time.time()
            self.__writer.start()
            while not self.__writer_run_event.is_set():
                if (time.time() - start_wait) > TIMEOUT:
                    msg = '%s - timed out waiting for process to start.'
                    msg = msg % str(self.__connection)
                    mcl.logging.sys.exception(self, msg)
                    raise Exception(msg)
                else:
                    time.sleep(0.01)

            # Log successful starting of thread.
            mcl.logging.sys.info(self, "%s - running", str(self.__connection))

            self.__is_alive = True
            return True

        else:
            return False

    def request_close(self):

        if self.is_alive():
            self.__writer_run_event.clear()
            self.__reader_run_event.clear()

    def close(self):
        """Close connection to queued listener.

        Returns:
            :class:`bool`: Returns :data:`True` if the queued listener was
                           closed. If the queued listener was already closed,
                           the request is ignored and the method returns
                           :data:`False`.

        """

        # Stop queuing broadcasts on process.
        if self.is_alive():
            mcl.logging.sys.info(self, "%s - stop request",
                                 str(self.__connection))

            # Send signal to STOP queue WRITER and READER.
            self.__writer_run_event.clear()
            self.__reader_run_event.clear()

            # Wait for queue READER to terminate.
            self.__reader.join(TIMEOUT)
            if self.__reader.is_alive():
                msg = "%s - timed out waiting for thread to stop."
                msg = msg % str(self.__connection)
                mcl.logging.sys.exception(self, msg)
                raise Exception(msg)

            # Wait for queue WRITER to terminate.
            self.__writer.join(TIMEOUT)
            if self.__writer.is_alive():
                msg = "%s - timed out waiting for process to stop."
                msg = msg % str(self.__connection)
                mcl.logging.sys.exception(self, msg)
                raise Exception(msg)

            # Log successful shutdown of thread and process.
            mcl.logging.sys.info(self, "%s - stopped", str(self.__connection))

            # Reset asynchronous objects (Drop data in the queue).
            self.__queue = None
            self.__reader = None
            self.__writer = None
            self.__is_alive = False
            return True
        else:
            return False
