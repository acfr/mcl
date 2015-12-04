"""Base class for implementing the event-driven programming paradigm.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>

"""


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

    """

    def __init__(self):
        """Document the __init__ method at the class level."""

        # Store callbacks in a list.
        self.__callbacks = list()

    def is_subscribed(self, callback):
        """Return whether a callback is registered with this object.

        Args:
            callback (function): The callback to test for registration.

        Returns:
            bool: Returns ``True`` if the callback has been registered with
                  this object. Returns ``False`` if the callback has NOT been
                  registered with this object.

        """

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
            self.__callbacks.append(callback)
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
            *args (any): mandatory arguments to send to callback functions.
            **kwargs (any): key-word arguments to send to callback functions.

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
