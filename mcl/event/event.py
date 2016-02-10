"""Module for implementing the event-driven programming paradigm.

The :py:mod:`~.event.event` module provides a means for implementing
event-driven programming. This is done through the :py:class:`.Event` object.

The :py:class:`.Event` object allows data to be communicated to callback
methods via the :py:meth:`.__trigger__` method. Callback methods can
un/subscribe to the :py:class:`.Event` object via the :py:meth:`.unsubscribe`
and :py:meth:`.subscribe` methods.

Example usage:

.. testcode::

    import os
    from mcl import Event

    pub = Event()
    pub.subscribe(lambda data: os.sys.stdout.write(data + '\\n'))
    pub.__trigger__('Hello world')

.. testoutput::
   :hide:

   Hello world

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>

"""


class Event(object):
    """Class for issuing events and triggering callback functions."""

    def __init__(self):
        self.__callbacks = list()

    # Overload with pass through to hide documentation.
    def __weakref__(self, *args, **kwargs):
        super(Event, self).__weakref__(*args, **kwargs)

    def is_subscribed(self, callback):
        """Return whether a callback is registered with this object.

        Args:
            callback (function): The callback to test for registration.

        Returns:
            bool: Returns :py:data:`.True` if the callback has been registered
                with this object. Returns :py:data:`.False` if the callback has
                NOT been registered with this object.

        """

        return callback in self.__callbacks

    def subscribe(self, callback):
        """Subscribe a callback to events.

        Args:
            callback (function): The callback to execute on a event.

        Returns:
            bool: Returns :py:data:`.True` if the callback was successfully
                registered. If the callback already exists in the list of
                callbacks, it will not be registered again and
                :py:data:`.False` will be returned.

        Raises:
            TypeError: If the input callback does not have a '__call__' method,
                a :py:exc:`.TypeError` is raised.

        """

        # Check that we can actually call this callback.
        if not hasattr(callback, '__call__'):
            raise TypeError("Callback must contain a '__call__' method.")

        # Add callback if it does not exist and start processing callback data
        # on a thread.
        if not self.is_subscribed(callback):
            self.__callbacks.append(callback)
            return True

        # Do not add the callback if it already exists.
        else:
            return False

    def unsubscribe(self, callback):
        """Unsubscribe a callback from events.

        Args:
            callback (function): The callback to be removed from event
                notifications.

        Returns:
            bool: Returns :py:data:`.True` if the callback was successfully
                removed. If the callback does not exist in the list of
                callbacks, it will not be removed and :py:data:`.False` will be
                returned.

        """

        # If the callback exists, stop processing data and remove the callback.
        if self.is_subscribed(callback):
            self.__callbacks.remove(callback)
            return True

        # No need to add the callback if it does not exist.
        else:
            return False

    def num_subscriptions(self):
        """Return the number of registered callbacks.

        Returns:
            int: number of registered callbacks.

        """

        return len(self.__callbacks)

    def __trigger__(self, *args, **kwargs):
        """Trigger an event and issue data to the callback functions.

        Args:
            *args: Arbitrary mandatory arguments to send to callback functions.
            **kwargs: Arbitrary keyword arguments to send to callback
                functions.

        """

        # Copy list of callbacks before iterating. This allows the list to be
        # modified from within a callback method. From the Python tutorial:
        #
        #     If you need to modify the sequence you are iterating over while
        #     inside the loop (for example to duplicate selected items), it is
        #     recommended that you first make a copy. Iterating over a sequence
        #     does not implicitly make a copy. The slice notation, [:], makes
        #     this especially convenient.
        #
        # Reference:
        #
        #     https://docs.python.org/2/tutorial/controlflow.html#for-statements
        #
        for callback in self.__callbacks[:]:
            callback(*args, **kwargs)
