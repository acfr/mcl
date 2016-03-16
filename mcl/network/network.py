"""Generic interface for publishing and receiving data in MCL.

This module provides generic methods and objects for creating network
broadcasters and listeners from :class:`~.abstract.Connection` or
:class:`.Message` objects. Since the details of interface connections are
encapsulated by :class:`~.abstract.Connection` objects, the specific
implementation of broadcasters and listeners can be abstracted
away. Broadcasting and receiving data from MCL networks can be handled by
generic functions and objects.

The functions and objects provided in this module provide generic tools for
interacting with MCL network interfaces. The only implementation specific
objects that need to be used, when writing applications, are
:class:`~.abstract.Connection` objects (e.g. :class:`.udp.Connection`).

Broadcasters and listeners can be created using a
:class:`~.abstract.Connection` object with the following functions:

    - :func:`.RawBroadcaster`
    - :func:`.RawListener`

Example usage:

.. testcode::

    import os
    import time
    from mcl.network.udp import Connection
    from mcl.network.network import RawListener
    from mcl.network.network import RawBroadcaster

    # Create raw listener and broadcaster from IPv6 connection.
    URL = 'ff15::c75d:ce41:ea8e:a000'
    connection = Connection(URL)
    listener = RawListener(connection)
    broadcaster = RawBroadcaster(connection)

    # Print received data to screen.
    listener.subscribe(lambda d: os.sys.stdout.write(d['payload']))

    # Broadcast data.
    broadcaster.publish('hello world')
    time.sleep(0.1)

    # Close connections.
    listener.close()
    broadcaster.close()

.. testoutput::
   :hide:

   hello world

Similarly, *message* broadcasters and listeners can be created using a
:class:`.Message` object with the following objects:

    - :class:`.MessageBroadcaster`
    - :class:`.MessageListener`

Example usage:

.. testcleanup:: send-receive

    # WARNING: this should not be deployed in production code. It is an
    #          abuse that has been used for the purposes of doc-testing.
    mcl.message.messages._MESSAGES = list()

.. testcode:: send-receive

    import os
    import time
    import mcl.message.messages
    from mcl.network.udp import Connection
    from mcl.network.network import MessageListener
    from mcl.network.network import MessageBroadcaster

    # Define MCL message.
    class ExampleMessage(mcl.message.messages.Message):
        mandatory = ('text',)
        connection = Connection('ff15::c75d:ce41:ea8e:b000')

    # Create raw listener and broadcaster from IPv6 connection.
    listener = MessageListener(ExampleMessage)
    broadcaster = MessageBroadcaster(ExampleMessage)

    # Print received message to screen.
    listener.subscribe(lambda d: os.sys.stdout.write(d['payload']['text']))

    # Broadcast message.
    broadcaster.publish(ExampleMessage(text='hello world'))
    time.sleep(0.1)

    # Close connections.
    listener.close()
    broadcaster.close()

.. testoutput:: send-receive
   :hide:

   hello world

The object :class:`.QueuedListener` can operate as a :func:`.RawListener` or a
:class:`.MessageListener` depending on the input. This object differs from
other listener objects by receiving network data on a separate process and
inserting the data to a multi-processing queue. Callbacks are issued on a
separate thread. The intention is to use a light-weight process for receiving
data so as to achieve more accurate timing by avoiding restrictions of the GIL
- resource allocation is left to the operating system. This object maintains
the same interface as other listener objects.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import time
import Queue
import datetime
import threading
import multiprocessing

import mcl.event.event
import mcl.network.abstract
import mcl.message.messages

# Time to wait for threads and processes to start/stop. This parameter could be
# exposed to the user. Currently it is viewed as an unnecessary tuning
# parameter.
TIMEOUT = 10


def _set_process_name(name):                                 # pragma: no cover
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

    Example usage:

    .. testcode::

        from mcl.network.udp import Connection
        from mcl.network.network import RawBroadcaster

        # Create raw broadcaster from IPv6 connection.
        URL = 'ff15::c75d:ce41:ea8e:0a00'
        connection = Connection(URL)
        broadcaster = RawBroadcaster(connection)

        # Broadcast data.
        broadcaster.publish('hello world')

        # Close connection.
        broadcaster.close()

    Args:
        connection (:class:`~.abstract.Connection`): Connection object.
        topic (str): Topic associated with the network interface.

    Attributes:
        connection (:class:`~.abstract.Connection`): Connection object.
        topic (str): Topic associated with the network interface broadcasts.
        is_open (bool): Returns :data:`True` if the network interface is
            open. Otherwise returns :data:`False`.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    # Ensure the connection object is properly specified.
    if not isinstance(connection, mcl.network.abstract.Connection):
        msg = "The argument 'connection' must be an instance of a "
        msg += "Connection()."
        raise TypeError(msg)

    try:
        return connection.broadcaster(connection, topic=topic)
    except:
        raise


def RawListener(connection, topics=None):
    """Return an object for receiving data over a network interface.

    Objects returned by :func:`.RawListener` make network data available to
    subscribers by issuing callbacks, when data arrives, in the following
    format::

        {'topic': str,
         'payload': obj()}

    where:

        - **<topic>** is a string containing the topic associated with the
          received data.

        - **<payload>** is the received (serialisable) data.

    Example usage:

    .. testcode::

        import os
        from mcl.network.udp import Connection
        from mcl.network.network import RawListener

        # Create raw listener from IPv6 connection.
        URL = 'ff15::c75d:ce41:ea8e:0b00'
        connection = Connection(URL)
        listener = RawListener(connection)

        # Print received data to screen.
        listener.subscribe(lambda d: os.sys.stdout.write(d['payload']))

        # Close connection.
        listener.close()

    Args:
        connection (:class:`~.abstract.Connection`): Connection object.
        topics (str or list): Topics associated with the network interface
            represented as either a string or list of strings.

    Attributes:
        connection (:class:`~.abstract.Connection`): Connection object.
        topics (str or list): Topics associated with the network interface.
        is_open (bool): Returns :data:`True` if the network interface is
            open. Otherwise returns :data:`False`.

    Raises:
        TypeError: If any of the inputs are ill-specified.

    """

    # Ensure the connection object is properly specified.
    if not isinstance(connection, mcl.network.abstract.Connection):
        msg = "The argument 'connection' must be an instance of a "
        msg += "Connection()."
        raise TypeError(msg)

    try:
        return connection.listener(connection, topics=topics)
    except:
        raise


class MessageBroadcaster(object):
    """Send messages over a network interface.

    The :class:`.MessageBroadcaster` object is a factory which manufactures
    objects for broadcasting MCL :class:`.Message` objects over a network. The
    returned object overloads the :meth:`~.abstract.RawBroadcaster.publish` of
    a :class:`~.abstract.RawBroadcaster` object to serialise the contents of a
    :class:`.Message` before transmission. `Message pack
    <http://msgpack.org/index.html>`_ is used to serialise :class:`.Message`
    objects into byte string.

    Example usage:

    .. testcleanup:: broadcast

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of unit-testing.
        mcl.message.messages._MESSAGES = list()

    .. testcode:: broadcast

        import mcl.message.messages
        from mcl.network.udp import Connection
        from mcl.network.network import MessageBroadcaster

        # Define MCL message.
        class ExampleMessage(mcl.message.messages.Message):
            mandatory = ('text',)
            connection = Connection('ff15::c75d:ce41:ea8e:00a0')

        # Create message broadcaster from IPv6 connection.
        broadcaster = MessageBroadcaster(ExampleMessage)

        # Broadcast message.
        broadcaster.publish(ExampleMessage(text='hello world'))

        # Close connection.
        broadcaster.close()

    For a list of available methods and attributes in the returned object, see
    :class:`~.abstract.RawBroadcaster`.

    Args:
        message (:class:`.Message`): MCL message object.
        topic (str): Topic associated with the network interface.

    """

    def __new__(cls, message, topic=None):

        # Ensure 'message' is a Message() object.
        msg = "'message' must reference a Message() sub-class."
        try:
            if not issubclass(message, mcl.message.messages.Message):
                raise TypeError(msg)
        except:
            raise TypeError(msg)

        # Use closure to define a sub-class of the RawBroadcaster specified in
        # the message connection. Once defined, return an instance of the new
        # MessageBroadcaster() sub-class.
        message_type = message

        class MessageBroadcaster(message.connection.broadcaster):
            """Send messages over a network interface.

            The :class:`.MessageBroadcaster` object provides an interface for
            broadcasting MCL :class:`.Message` objects over a
            network. :class:`.MessageBroadcaster` establishes a network
            connection using the information contained within the input
            :class:`.Message` type.

            For a list of available methods and attributes, see
            :class:`~.abstract.RawBroadcaster`.

            Args:
                message (:class:`.Message`): MCL message object.
                topic (str): Topic associated with the network interface.

            """

            def publish(self, message, topic=None):
                """Send an MCL message over the network.

                Args:
                    message (:class:`.Message`): MCL message object.
                    topic (str): Topic associated with published message. This
                        option will temporarily override the topic specified
                        during instantiation.

                Raises:
                    TypeError: If the input message type differs from the
                        message specified during instantiation.

                """

                # Ensure 'message' is a Message() object.
                if not isinstance(message, message_type):
                    error_msg = "Input 'msg' must reference a %s() instance."
                    raise TypeError(error_msg % message_type.__name__)

                # Publish data.
                super(MessageBroadcaster, self).publish(message, topic=topic)

        return MessageBroadcaster(message.connection, topic=topic)


class MessageListener(object):
    """Receive messages over a network interface.

    The :class:`.MessageListener` object is a factory which manufactures
    objects for receiving MCL :class:`.Message` objects over a network. The
    returned object inherits from the :class:`~.abstract.RawListener`
    class. When data is received, it is decoded into a :class:`.Message` object
    before an event is raised to forward the received data to subscribed
    callbacks in the following format::

        {'topic': str,
         'payload': Message()}

    where:

        - **<topic>** is a string containing the topic associated with the
          received data.

        - **<payload>** is the received :class:`.Message` object.

    Example usage:

    .. testcleanup:: listen

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of doc-testing.
        mcl.message.messages._MESSAGES = list()

    .. testcode:: listen

        import mcl.message.messages
        from mcl.network.udp import Connection
        from mcl.network.network import MessageListener

        # Define MCL message.
        class ExampleMessage(mcl.message.messages.Message):
            mandatory = ('text',)
            connection = Connection('ff15::c75d:ce41:ea8e:00b0')

        # Create message listener from IPv6 connection.
        listener = MessageListener(ExampleMessage)

        # Print received message to screen.
        listener.subscribe(lambda d: os.sys.stdout.write(d['payload']['text']))

        # Close connection.
        listener.close()

    For a list of available methods and attributes in the returned object, see
    :class:`~.abstract.RawListener`.

    .. warning::

        :class:`.MessageListener` objects expect the transmitted data to be
        formatted as MCL :class:`.Message` objects. If the received data cannot
        be converted into a MCL :class:`.Message`, an exception will be raised
        on the I/O loop (thread) of the base
        :class:`~.abstract.RawListener`. This will prevent exceptions from
        being raised on the main thread and messages from being published. If
        no messages are being received, check the connections, ensure the data
        is being formatted correctly prior to transmission and refer to any
        stack-traces being printed on stdout.

    Args:
        message (:class:`.Message`): MCL message object.
        topics (str): List of strings containing topics
                      :class:`.MessageListener` will receive and process.

    """

    def __new__(cls, message, topics=None):

        # Ensure 'message' is a Message() object.
        if not issubclass(message, mcl.message.messages.Message):
            msg = "'message' must reference a Message() sub-class."
            raise TypeError(msg)

        # Use closure to define a sub-class of the RawListener specified in
        # the message connection. Once defined, return an instance of the new
        # MessageListener() sub-class.
        message_type = message

        class MessageListener(message.connection.listener):
            """Receive messages over a network interface.

            The :class:`.MessageListener` object provides an interface for
            receiving MCL :class:`.Message` objects over a
            network. :class:`.MessageListener` establishes a network connection
            using the information contained within the input :class:`.Message`
            type.

            For a list of available methods and attributes, see
            :class:`~.abstract.RawListener`.

            Args:
                message (:class:`.Message`): MCL message object.
                topic (str): Topic associated with the network interface.

            """

            def __trigger__(self, data):
                """Distribute MCL message to subscribed callbacks."""

                # Attempt to serialise input data.
                #
                # Note: Errors encountered during recieve will likely occur in
                #       the I/O loop of a network interface - and cannot be
                #       caught here.
                try:
                    data['payload'] = message_type(data['payload'])
                    super(MessageListener, self).__trigger__(data)
                except:                                      # pragma: no cover
                    raise

        return MessageListener(message.connection, topics=topics)


class QueuedListener(mcl.network.abstract.RawListener):
    """Open a broadcast address and listen for data.

    The :class:`.QueuedListener` object subscribes to a network broadcast and
    issues publish events when data is received.

    The difference between this object and other network listeners is that
    network data is received on a separate process and written to a
    multi-processing queue. The intention is to use a light-weight process to
    achieve more accurate timing by avoiding the GIL. Resource allocation is
    left to the operating system.

    A summary of the :class:`.QueuedListener` object is shown below::

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

        {'topic': str(),
         'payload': obj(),
         'time_received': datetime}

    where:

        - **<topic>** is a string representing the topic associated with the
          current data packet. This can be used for filtering broadcasts.

        - **<payload>** contains the contents of the data transmission.

        - **<time_received>** is a datetime object containing the time the data
          was received and queued.

    Example usage emulating objects returned from :func:`.RawListener`:

    .. testcode:: queuedlistener-raw

        import os
        import time
        from mcl.network.udp import Connection
        from mcl.network.network import QueuedListener
        from mcl.network.network import RawBroadcaster

        # Define MCL connection.
        URL = 'ff15::c75d:ce41:ea8e:00c0'
        connection = Connection(URL)

        # Print received data to screen.
        listener = QueuedListener(connection)
        broadcaster = RawBroadcaster(connection)
        listener.subscribe(lambda d: os.sys.stdout.write(d['payload']))

        # Broadcast data.
        broadcaster.publish('hello world')
        time.sleep(0.1)

        # Close connections.
        listener.close()

    .. testoutput:: queuedlistener-raw
       :hide:

       hello world

    Example usage emulating objects returned from
    :func:`.MessageListener`. Note how the interface is the same as the
    previous example but only the input object has changed:

    .. testcleanup:: queuedlistener-message

        # WARNING: this should not be deployed in production code. It is an
        #          abuse that has been used for the purposes of doc-testing.
        mcl.message.messages._MESSAGES = list()

    .. testcode:: queuedlistener-message

        import os
        import time
        import mcl.message.messages
        from mcl.network.udp import Connection
        from mcl.network.network import QueuedListener
        from mcl.network.network import MessageBroadcaster

        # Define MCL message.
        class ExampleMessage(mcl.message.messages.Message):
            mandatory = ('text',)
            connection = Connection('ff15::c75d:ce41:ea8e:00c0')

        # Print received message to screen.
        listener = QueuedListener(ExampleMessage)
        broadcaster = MessageBroadcaster(ExampleMessage)
        listener.subscribe(lambda d: os.sys.stdout.write(d['payload']['text']))

        # Broadcast message.
        broadcaster.publish(ExampleMessage(text='hello world'))
        time.sleep(0.1)

        # Close connections.
        listener.close()

    .. testoutput:: queuedlistener-message
       :hide:

       hello world

    Args:
        connection (:class:`~.abstract.Connection` or :class:`~.messages.Message`):
            an instance of a MCL connection object or a reference to a MCL
            message type.
        topics (str or list): Topics associated with the network interface
            represented as either a string or list of strings.
        open_init (bool): open connection immediately after initialisation.

    """

    def __init__(self, connection, topics=None, open_init=True):
        """Document the __init__ method at the class level."""

        try:
            msg = "'connection' must reference a Connection() instance "
            msg += "or a Message() subclass."

            # 'connection' is a Connection() instance.
            if isinstance(connection, mcl.network.abstract.Connection):
                self.__connection = connection
                self.__message_type = None

            # 'connection is a reference to a Message() subclass.
            elif issubclass(connection, mcl.message.messages.Message):
                self.__connection = connection.connection
                self.__message_type = connection
            else:
                raise TypeError(msg)
        except:
            raise TypeError(msg)

        # Attempt to initialise listener base-class.
        try:
            super(QueuedListener, self).__init__(self.__connection,
                                                 topics=topics)
        except:
            raise

        # To catch errors early, test if the RawListener() object can be
        # opened. RawListener() is created in the __enqueue method which is
        # executed on another thread. Propagating errors from there is more
        # difficult and occur later in the code execution.
        try:
            RawListener(self.__connection, topics=topics)
        except:
            raise

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
        if open_init:
            success = self.open()
            if not success:
                msg = "Could not connect to '%s'." % str(connection)
                raise IOError(msg)

    def is_open(self):
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
    def __enqueue(class_name, run_event, connection, topics, queue):
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
                data['time_received'] = datetime.datetime.utcnow()
                queue.put(data)
            except:
                pass

        # Start listening for network broadcasts.
        listener = RawListener(connection, topics=topics)

        # Capture broadcast data.
        listener.subscribe(enqueue)

        # Wait for user to terminate listening service.
        while run_event.is_set():
            try:
                time.sleep(0.25)
            except KeyboardInterrupt:                        # pragma: no cover
                break

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

                # Publish raw data.
                if self.__message_type is None:
                    self.__trigger__(data)

                # Publish message object.
                else:
                    try:
                        data['payload'] = self.__message_type(data['payload'])
                        self.__trigger__(data)

                    # Error during publishing.
                    except:
                        self.request_close()
                        raise

            # No data in queue.
            except Queue.Empty:
                pass

    def _open(self):
        """Open connection to queued listener and start publishing broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if a connection to the queued
                listener is opened. If the queued listener is already open, the
                request is ignored and the method returns :data:`False`.

        """

        # Start publishing broadcast-events on a thread.
        if not self.is_open():

            # Reset asynchronous objects.
            self.__queue = multiprocessing.Queue()

            # Create THREAD for dequeueing and publishing data.
            self.__reader_run_event.clear()
            self.__reader = threading.Thread(target=self.__dequeue)

            # Create PROCESS for enqueueing data.
            self.__writer_run_event.clear()
            self.__writer = multiprocessing.Process(target=self.__enqueue,
                                                    args=(self.__class__.__name__,
                                                          self.__writer_run_event,
                                                          self.__connection,
                                                          self.topics,
                                                          self.__queue))

            # Start asynchronous objects and wait for them to become alive.
            self.__writer.daemon = True
            self.__reader.daemon = True

            # Wait for queue READER to start.
            start_wait = time.time()
            self.__reader.start()
            while not self.__reader_run_event.is_set():      # pragma: no cover
                if (time.time() - start_wait) > TIMEOUT:
                    msg = '%s - timed out waiting for thread to start.'
                    msg = msg % str(self.__connection)
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            # Wait for queue WRITER to start.
            start_wait = time.time()
            self.__writer.start()
            while not self.__writer_run_event.is_set():      # pragma: no cover
                if (time.time() - start_wait) > TIMEOUT:
                    msg = '%s - timed out waiting for process to start.'
                    msg = msg % str(self.__connection)
                    raise Exception(msg)
                else:
                    time.sleep(0.1)

            self.__is_alive = True
            return True

        else:
            return False

    def open(self):
        """Open connection to queued listener and start publishing broadcasts.

        Returns:
            :class:`bool`: Returns :data:`True` if a connection to the queued
                listener is opened. If the queued listener is already open, the
                request is ignored and the method returns :data:`False`.

        """
        return self._open()

    def close(self):
        """Close connection to queued listener.

        Returns:
            :class:`bool`: Returns :data:`True` if the queued listener was
                closed. If the queued listener was already closed, the request
                is ignored and the method returns :data:`False`.

        """

        # Stop queuing broadcasts on process.
        if self.is_open():

            # Send signal to STOP queue WRITER and READER.
            self.__writer_run_event.clear()
            self.__reader_run_event.clear()

            # Wait for queue READER to terminate.
            self.__reader.join(TIMEOUT)
            if self.__reader.is_alive():                     # pragma: no cover
                msg = "%s - timed out waiting for thread to stop."
                msg = msg % str(self.__connection)
                raise Exception(msg)

            # Wait for queue WRITER to terminate.
            self.__writer.join(TIMEOUT)
            if self.__writer.is_alive():                     # pragma: no cover
                msg = "%s - timed out waiting for process to stop."
                msg = msg % str(self.__connection)
                raise Exception(msg)

            # Reset asynchronous objects (Drop data in the queue).
            self.__queue = None
            self.__reader = None
            self.__writer = None
            self.__is_alive = False
            return True
        else:
            return False
