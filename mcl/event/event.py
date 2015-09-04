"""Base class for implementing the event-driven programming paradigm.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>

"""

import copy
import Queue
import inspect
import threading
import traceback

QUEUE_TIMEOUT = 0.5


def _abstract_error(cls):
    """Function for throwing a NotImplementedError in virtual methods."""

    cls = cls.__class__.__name__
    fcn = traceback.extract_stack(None, 2)[0][2]
    message = "The method '%s' in '%s' is abstract. " % (fcn, cls)
    message += "Derived classes must override this method."

    return NotImplementedError(message)


class CallbackHandler(object):
    """Abstract class to represent callback objects."""

    def __init__(self):
        """Document the __init__ method at the class level."""
        pass

    def enqueue(self, data):
        """Virtual: Enqueue data to be processed by callback.

        Args:
            data (any): data to send to callback.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)

    def start(self):
        """Virtual: Start the callbacks' activity.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)

    def request_stop(self):
        """Virtual: Non-blocking signal to stop callback activity.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)

    def stop(self):
        """Virtual: Blocking signal to stop thread on next iteration.

        Raises:
            NotImplementedError: Virtual function must be implemented by
                                 subclasses.

        """
        raise _abstract_error(self)


class CallbackSynchronous(CallbackHandler):
    """Object for executing callbacks **synchronously**.

    Attributes:
        is_alive (boolean): Return whether the callback thread is alive
        is_stop_requested (boolean): Return whether the callback thread has
                                     been requested to stop

    """

    def __init__(self, callback):
        """Document the __init__ method at the class level."""

        super(CallbackSynchronous, self).__init__()
        self.__callback = callback
        self.__stop_requested = False
        self.__is_alive = False

    @property
    def is_alive(self):
        return self.__is_alive

    @property
    def is_stop_requested(self):
        return False

    def enqueue(self, data):
        """Enqueue data to be processed by callback.

        Args:
            data (any): data to send to callback.

        """

        if self.is_alive:
            self.__callback(data)

    def start(self):
        """Start the callbacks' activity.

        Returns:
            boolean: Returns ``True`` if the callback was started. If the
                     callback is already alive, the request is ignored and the
                     method returns ``False``.

        """

        if self.is_alive:
            return False
        else:
            self.__is_alive = True
            return True

    def request_stop(self):
        """Non-blocking signal to stop callback activity.

        Returns:

            boolean: Returns ``True`` if the request to stop was successful. If
                     the callback is already in a stopped state, the request is
                     ignored and the method returns ``False``.

        """

        if self.__is_alive:
            self.__is_alive = False
            return True
        else:
            return False

    def stop(self):
        """Blocking signal to stop callback activity.

        Returns:
            boolean: Returns ``True`` if the callback was stopped. If the
                     callback is already in a stopped state, the request is
                     ignored and the method returns ``False``.

        """

        return self.request_stop()


class CallbackAsynchronous(CallbackHandler):
    """Object for executing callbacks **asynchronously** on a thread.

    Attributes:
        is_alive (boolean): Return whether the callback thread is alive
        is_stop_requested (boolean): Return whether the callback thread has
                                     been requested to stop
        is_queue_empty (boolean): Return ``True`` if the callback queue is
                                  empty, ``False`` otherwise.

    """

    def __init__(self, callback):
        """Document the __init__ method at the class level."""

        super(CallbackAsynchronous, self).__init__()
        self.__callback = callback
        self.__queue = Queue.Queue()
        self.__stop_requested = threading.Event()
        self.__stop_requested.clear()

        self.__timeout = QUEUE_TIMEOUT
        self.__stop_requested.clear()
        self.__thrd = threading.Thread(target=self.__run,
                                       args=(self.__callback,
                                             self.__stop_requested,
                                             self.__queue,
                                             self.__timeout,))
        self.__thrd.daemon = True

    @property
    def is_alive(self):
        return self.__thrd.is_alive()

    @property
    def is_stop_requested(self):
        return self.__stop_requested.isSet()

    @property
    def is_queue_empty(self):
        return self.__queue.empty()

    def enqueue(self, data):
        """Enqueue data to be processed by callback.

        Args:
            data (any): data to send to callback.

        """
        self.__queue.put(data)

    def start(self):
        """Start the callbacks' activity.

        Returns:
            boolean: Returns ``True`` if the callback was started. If the
                     callback is already alive, the request is ignored and the
                     method returns ``False``.

        """

        if self.is_alive:
            return False
        else:
            self.__thrd.start()
            return True

    def __run(self, callback, stop_requested, queue, timeout):
        """Method to execute whenever data is available."""

        while not stop_requested.is_set():
            try:
                data = queue.get(True, timeout)
                callback(data)
            except Queue.Empty:
                continue

        stop_requested.clear()

    def request_stop(self):
        """Non-blocking signal to stop callback activity.

        Returns:

            boolean: Returns ``True`` if the request to stop was successful. If
                     the callback is already in a stopped state, the request is
                     ignored and the method returns ``False``.

        """

        if self.is_alive and not self.__stop_requested.is_set():
            self.__stop_requested.set()
            return True
        else:
            return False

    def stop(self):
        """Blocking signal to stop callback activity.

        Returns:
            boolean: Returns ``True`` if the callback was stopped. If the
                     callback is already in a stopped state, the request is
                     ignored and the method returns ``False``.

        """

        was_stopped = self.request_stop()
        self.__thrd.join()

        return was_stopped


class Publisher(object):
    """Class for issuing events and triggering callback functions.

    The :py:class:`.Publisher` object provides a means for implementing
    event-driven programming.

    The :py:class:`.Publisher` object allows data to be communicated to
    callback methods via the :py:meth:`.publish` method. Callback methods can
    un/subscribe to the :py:class:`.Publisher` object via the
    :py:meth:`.unsubscribe` and :py:meth:`.subscribe` methods. Callback
    functions must accept only one input argument - the data which is issued by
    :py:class:`.Publisher`.

    Example usage::

        import os
        from mcl import Publisher

        pub = Publisher()
        pub.subscribe(lambda data: os.sys.stdout.write(str(data) + '\\n'))
        pub.publish('Hello world')


    Args:
        callbackhandler (:py:class:`.CallbackHandler`, optional): Object for
        handling callbacks.

    """

    def __init__(self, callbackhandler=CallbackSynchronous):
        """Document the __init__ method at the class level."""

        # Check that callback handler has expected properties.
        if ((not inspect.isclass(callbackhandler)) or
           (not issubclass(callbackhandler, CallbackHandler))):
            msg = "Callbackhandler must be a subclass of 'CallbackHandler'."
            raise TypeError(msg)

        # Store method for executing and handling callbacks.
        self.__callbackhandler = callbackhandler

        # Store callbacks in a dictionary.
        self.__callbacks = dict()

        # Provide lock for accessing callbacks.
        self.__callback_lock = threading.Lock()

    def is_subscribed(self, callback):
        """Return whether a callback is registered with this object.

        Args:
            callback (function): The callback to test for registration.

        Returns:
            bool: Returns ``True`` if the callback has been registered with
                  this object. Returns ``False`` if the callback has NOT been
                  registered with this object.

        """
        with self.__callback_lock:
            return callback in self.__callbacks

    def subscribe(self, callback):
        """Subscribe to events.

        Args:
            callback (function): The callback to execute on a event.

        Returns:
            bool: Returns ``True`` if the callback was successfully
                  registered. If the callback already exists in the list of
                  callbacks, it will not be registered again and ``False`` will
                  be returned.

        Raises:
            TypeError: If the input callback does not have a '__call__' method,
                       a TypeError is raised.

        """

        # Note: Ask for permission! Ducktyping will cause headaches if
        # thread/processes do not have properly implemented start/stop
        # methods. Catch errors early rather than late...

        # Check that we can actually call this callback.
        if not hasattr(callback, '__call__'):
            raise TypeError("Callback must contain a '__call__' method.")

        # Add callback if it does not exist and start processing callback data
        # on a thread.
        if not self.is_subscribed(callback):
            with self.__callback_lock:
                self.__callbacks[callback] = self.__callbackhandler(callback)
                self.__callbacks[callback].start()

            return True

        # Do not add the callback if it already exists.
        else:
            return False

    def unsubscribe(self, callback):
        """Unsubscribe to events.

        Args:
            callback (function): The callback to be removed from event
                                 notifications.

        Returns:
            bool: Returns ``True`` if the callback was successfully removed. If
                  the callback does not exist in the list of callbacks, it will
                  not be removed and ``False`` will be returned.

        """

        # If the callback exists, stop processing data and remove the callback.
        if self.is_subscribed(callback):
            with self.__callback_lock:
                self.__callbacks[callback].stop()
                del self.__callbacks[callback]
            return True

        # No need to add the callback if it does not exist.
        else:
            return False

    def num_subscriptions(self):
        """Return the number of registered callbacks.

        Returns:
            int: number of registered callbacks.

        """
        with self.__callback_lock:
            return len(self.__callbacks)

    def publish(self, data):
        """Issue an event and communicat data to the callback functions.

        Args:
            data (any): Data to send to the registered callbacks.

        """
        # Make a copy of the callback list so we avoid deadlocks
        with self.__callback_lock:
            callbacks = copy.copy(self.__callbacks)

        for callback in callbacks:
            callbacks[callback].enqueue(data)
