"""Base class for implementing the event-driven programming paradigm.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>

"""
import abc
import copy
import inspect
import threading
from abc import abstractmethod

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
    :py:meth:`.unsubscribe` and :py:meth:`.subscribe` methods. Callback
    functions must accept only one input argument - the data which is issued by
    :py:class:`.Event`.

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

    def trigger(self, *args, **kwargs):
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


class _HideTriggerMeta(type):
    """Meta-class for making the Event.trigger() method 'private'.

    The :py:class:`._HideTriggerMeta` object is a meta-class designed to hide
    the :py:func:`Event.trigger` method in :py:class:`.RawListener`
    sub-classes.

    Implementations of :py:class:`.RawListener` should link the
    :py:func:`Event.trigger` method to I/O events such as receiving data on a
    network interface. While this allows the sub-classes to be event-driven,
    the :py:func:`Event.trigger` method is exposed.

    This design pattern is an interface design choice that allows
    :py:class:`.RawListener` objects to inherit functionality from
    :py:class:`.Event` without obvious exposure of the :py:func:`Event.trigger`
    method. The goal is to discourage *users* of :py:class:`.RawListener`
    objects from forcing data to subscribed callback methods by direct calls to
    the :py:func:`Event.trigger` method. *Developers* of
    :py:class:`.RawListener` objects will need to call the slightly more
    obscure '__trigger__' method from I/O loops.

    .. note::

        This meta-class inherits from the abc.ABCMeta object. As such, objects
        which implement this meta-class will gain the functionality of abstract
        base classes.

    """

    def __new__(cls, name, bases, dct):

        # Note: Inherting from Event() is not necessary in RawListener() since
        #       this meta-class redefines the base classes in the following
        #       code. The sub-class syntax has been left inplace as a reminder
        #       that RawListener() objects behave like Event() objects.
        #
        if bases == (Event,):

            # Copy Event() object data and rename the trigger method to make it
            # 'private'.
            class_dict = dict(Event.__dict__)
            class_dict['__trigger__'] = class_dict['trigger']
            class_dict['__trigger__'].func_name = '__trigger__'
            del(class_dict['trigger'])

            # Override base with 'private' Event() object..
            bases = (type('PrivateEvent', Event.__bases__, class_dict), )

        return super(_HideTriggerMeta, cls).__new__(cls, name, bases, dct)
