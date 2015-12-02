"""Base class for implementing the event-driven programming paradigm.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>

"""
import abc
import copy
import inspect
import threading
from abc import abstractmethod
from collections import OrderedDict

QUEUE_TIMEOUT = 0.5


class Callback(object):
    """Abstract class for representing callback objects."""

    # Ensure abstract methods are redefined in sub-classes.
    __metaclass__ = abc.ABCMeta

    @abstractmethod
    def __call__(self, *args, **kwargs):
        """Virtual: Called when the callback is ''called'' as a function

        Args:
            *args (any): mandatory arguments to send to callback function.
            **kwargs (any): key-word arguments to send to callback function.

        """
        pass


class CallbackSequential(Callback):
    """Object for executing callbacks **sequentially**."""

    def __init__(self, callback):
        """Document the __init__ method at the class level."""

        self.__callback = callback

    def __call__(self, *args, **kwargs):
        """Execute callback  when ''called'' as a function.

        Args:
            *args (any): mandatory arguments to send to callback function.
            **kwargs (any): key-word arguments to send to callback function.

        """

        self.__callback(*args, **kwargs)


class CallbackConcurrent(Callback):
    """Object for executing callbacks **concurrently** on a new thread."""

    def __init__(self, callback):
        """Document the __init__ method at the class level."""

        self.__callback = callback

    def __call__(self, *args, **kwargs):
        """Execute callback on a new thread when ''called'' as a function.

        Args:
            *args (any): mandatory arguments to send to callback function.
            **kwargs (any): key-word arguments to send to callback function.

        """

        # Execute callback on a new thread.
        thread = threading.Thread(target=self.__callback,
                                  args=args, kwargs=kwargs)
        thread.daemon = True
        thread.start()


class Event(object):
    """Class for issuing events and triggering callback functions.

    The :py:class:`.Event` object provides a means for implementing
    event-driven programming.

    The :py:class:`.Event` object allows data to be communicated to callback
    methods via the :py:meth:`.publish` method. Callback methods can
    un/subscribe to the :py:class:`.Event` object via the
    :py:meth:`.unsubscribe` and :py:meth:`.subscribe` methods.

    Example usage::

        import os
        from mcl import Event

        pub = Event()
        pub.subscribe(lambda data: os.sys.stdout.write(str(data) + '\\n'))
        pub.publish('Hello world')


    Args:
        callback (:py:class:`.Callback`, optional): Object for handling
            callbacks.

    """

    def __init__(self, callback=CallbackSequential):
        """Document the __init__ method at the class level."""

        # Check that callback handler has expected properties.
        if ((not inspect.isclass(Callback)) or
            (not issubclass(callback, Callback))):
            msg = "'callback' must be a subclass of Callback()."
            raise TypeError(msg)

        # Store method for executing and handling callbacks.
        self.__Callback = callback

        # Store callbacks in a dictionary.
        self.__callbacks = OrderedDict()

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

        # Check that we can actually call this callback.
        if not hasattr(callback, '__call__'):
            raise TypeError("Callback must contain a '__call__' method.")

        # Add callback if it does not exist and start processing callback data
        # on a thread.
        if not self.is_subscribed(callback):
            with self.__callback_lock:
                self.__callbacks[callback] = self.__Callback(callback)
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

    def __trigger__(self, *args, **kwargs):
        """Trigger an event and issue data to the callback functions.

        Args:
            *args (any): mandatory arguments to send to callback functions.
            **kwargs (any): key-word arguments to send to callback functions.

        """

        # Make a copy of the callback list so we avoid deadlocks
        with self.__callback_lock:
            callbacks = copy.copy(self.__callbacks)

        for callback in callbacks:
            callbacks[callback](*args, **kwargs)
