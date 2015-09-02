"""Object specification for creating messages in MCL.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import json
import msgpack
import datetime
from sets import Set


# Globally track Message() definitions. The meta-class _RegisterMeta() inserts
# Message() definitions into _MESSAGES when Message() objects are subclassed.
_MESSAGES = list()


class _RegisterMeta(type):
    """Meta-class for globally registering Message() objects.

    :py:class:`._RegisterMeta` is a simple meta-class that maintains a global
    register of :py:class:`.Message` sub-classes. :py:class:`.Message`
    subclasses are added to the register when they are defined. During this
    process :py:class:`._RegisterMeta` also checks to see if a
    :py:class:`.Message` object with the same name has already been defined.

    Note that the list of :py:class:`.Message` sub-classes can be acquired by
    calling::

        messages = Message.__subclasses__()

    The reason the :py:class:`._RegisterMeta` is preferred is that it can
    provide error checking at the time of definition. Note that subclasses
    cannot easily be removed from the list returned by
    ``Message.__subclasses__()``. By using this meta-class,
    :py:class:`.Message` objects can be removed from the global register via
    other methods (see :py:func:`.remove_message_object`).

    Args:
      cls (class): is the class being instantiated.
      name (string): is the name of the new class.
      bases (tuple): base classes of the new class.
      dct (dict): dictionary mapping the class attribute names to objects.

    """

    def __init__(cls, name, bases, dct):

        # Do not allow Message() objects with the name Message() to be added.
        if name == 'Message' and len(_MESSAGES) > 0:
            msg = 'Cannot redefine the base Message() object.'
            raise Exception(msg)

        # Add new Message() definitions.
        elif name != 'Message':

            # Check that a message with the same name does not exist.
            if name in [message.__name__ for message in _MESSAGES]:
                msg = "A Message() with the name '%s' already exists."
                raise Exception(msg % name)

            # Store message definition.
            _MESSAGES.append(cls)

        super(_RegisterMeta, cls).__init__(name, bases, dct)


class Message(dict):
    """Base class that is used for message passing.

    Has a set of mandatory fields that must be present. Additional fields will
    be serialised if present, but are not required.

    """

    __metaclass__ = _RegisterMeta

    def __init__(self, mandatory_items, *args, **kwargs):

        self.__mandatory_items = mandatory_items

        # If no inputs were passed into the constructor, initialise the object
        # with empty fields.
        if not args and not kwargs:
            empty = [None] * len(self.__mandatory_items)
            kwargs = dict(zip(self.__mandatory_items, empty))

        # Initialise message object with items.
        super(Message, self).__init__(**kwargs)
        super(Message, self).__setitem__('name', self.__class__.__name__)
        self.update(*args, **kwargs)
        self.__check_requirements(self)

    def __check_requirements(self, dct):
        """Ensure Message contains all items in the 'mandatory_items' list."""

        if not Set(dct.keys()).issuperset(Set(self.__mandatory_items)):
            msg = "'%s' must have the following items: [" % self['name']
            msg += ', '.join(self.__mandatory_items)
            msg += '].'
            raise TypeError(msg)

    def __set_time(self):
        """Update the CPU time-stamp in milliseconds from UTC epoch.

        Note: This method should be platform independent and provide more
        precision than the time.time() method. See:

        https://docs.python.org/2/library/datetime.html#datetime.datetime.now

        """
        time_now = datetime.datetime.now()
        time_origin = datetime.datetime.utcfromtimestamp(0)
        timestamp = (time_now - time_origin).total_seconds()
        super(Message, self).__setitem__('timestamp', timestamp)

    def mandatory_items(self):
        """Return all mandatory items for this message type."""

        return self.__mandatory_items

    def encode(self):
        """Return the compressed form of the message."""

        return msgpack.dumps(self)

    def to_json(self):
        """Convert into JSON format."""
        return json.dumps(self)

    def __decode(self, packed_dictionary):
        """Unpack serialised data."""
        msg = None
        try:
            dct = msgpack.loads(packed_dictionary)
            missing = Set(self.__mandatory_items) - Set(dct.keys())

            # Transmitted object was decoded but is not a dictionary.
            if type(dct) is not dict:
                msg = 'Serialised object is not a dictionary.'

            # Transmitted object is missing mandatory fields.
            elif len(missing) > 0:
                msg = 'The transmitted object was missing the following '
                msg += 'mandatory items: [' + ', '.join(missing) + '].'

            # Decode was successful.
            else:
                return dct

        # Decoding was unsuccessful.
        # pylint: disable=W0702
        except Exception as e:
            msg = "Could not unpack message."
            print (e)

        # Raise error encountered during unpacking.
        raise TypeError(msg)

    def __from_binary(self, packed_dictionary):
        """Expand a packed dictionary."""

        dct = self.__decode(packed_dictionary)
        super(Message, self).update(dct)

    def __setitem__(self, key, value):
        """Set an item to a new value.

        Note: this will not update message name or timestamp.

        """

        # Prevent write access to Message name and timestamp.
        if key == 'name' and key in self:
            msg = "The key value '%s' in '%s' is read-only."
            raise ValueError(msg % (key, self['name']))

        # All other items can be accessed normally.
        else:
            super(Message, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        """Update message with dictionary of new values."""

        # Set the default timestamp to None. If it is updated by the passed in
        # arguments, we won't update it automatically.
        self['timestamp'] = None

        if len(args) > 1:
            msg = 'Input argument must be a msgpack serialised dictionary '
            msg += 'OR a python dictionary.'
            raise TypeError(msg)

        # Update with keyword arguments.
        if not args and kwargs:
            super(Message, self).update(**kwargs)

        # Update message with a serialised dictionary.
        elif (len(args) == 1) and (type(args[0]) is str):
            self.__from_binary(args[0])
            return

        # Update message with a dictionary (and keyword arguments).
        elif (len(args) == 1) and (type(args[0]) is dict):

            # Update message with a dictionary and keyword arguments.
            if kwargs:
                super(Message, self).update(*args, **kwargs)

            # Only use dictionary.
            else:
                super(Message, self).update(*args)

        # Invalid input type.
        else:
            msg = 'Input argument must be a msgpack serialised dictionary '
            msg += 'OR a python dictionary.'
            raise TypeError(msg)

        # Record the time of update.
        if not self['timestamp']:
            self.__set_time()


def remove_message_object(name):
    """De-register Message() object from list of known messages.

    Args:
        name (string): Name of message object to de-register.

    Returns:
        bool: ``True`` if the Message() object was de-registered. ``False`` if
            the Message() object does not exist.

    """

    # Create name of available messages.
    names = [msg.__name__ for msg in _MESSAGES]

    # The message exists, remove it from the list.
    if name in names:
        index = names.index(name)
        del _MESSAGES[index]

        return True

    # The message does not exist. No action required.
    else:
        return False


def list_messages(names=False):
    """List message objects derived from Message.

    Args:
        name (boolean, **optional**): By default (``False``) a list of message
            objects derived from :py:class:`.Message` is returned. If set to
            ``True``, a tuple containing a list of message objects derived from
            :py:class:`.Message` and list of their names as a string is
            returned.

    Returns:
        list or tuple: a list of message objects derived from
            :py:class:`.Message` is returned. If ``name`` is set to ``True``, a
            tuple containing a list of message objects derived from
            :py:class:`.Message` and list of their names as a string is
            returned.

    """

    # Create soft copy of _MESSAGES so that _MESSAGES cannot be altered
    # directly.
    messages = [msg for msg in _MESSAGES]

    # Get message names.
    if names:
        message_names = list()
        for message in messages:
            message_names.append(message.__name__)

        return messages, message_names

    # Return message objects.
    else:
        return messages


def get_message_objects(names):
    """Return message object(s) from name(s).

    Args:
        name (string or list): A single (string) or multiple (list/tuple of
            strings) message object name(s) to retrieve.

    Returns:
        :py:class:`.Message` or list: If a single message object is requested
            (string input), the requested message object is returned. If
            multiple message objects are requested, a list of message objects
            is returned.

    Raises:
        TypeError: If ``names`` is not a string or list/tuple of strings.
        NameError: If ``names`` does not exist or multiple message objects are
            found.

    """

    # Input is a string.
    if isinstance(names, basestring):

        # Create name of available messages.
        messages = [(msg, msg.__name__) for msg in _MESSAGES]

        # Cache messages with a matching name.
        matches = list()
        for message in messages:
            if message[1] == names:
                matches.append(message)

        # Message does not exist.
        if len(matches) == 0:
            raise NameError("Could locate the message named: '%s'." % names)

        # Multiple messages with the same name exist.
        elif len(matches) > 1:
            msg = "Multiple messages named '%s' found including:\n" % names
            for message in matches:
                msg += '    %s.%s\n' % (message[0].__module__, message[1])
            raise NameError(msg)

        # Return unique message.
        return matches[0][0]

    # Input is a list or tuple.
    elif isinstance(names, (list, tuple)):

        messages = list()
        for name in names:
            try:
                messages.append(get_message_objects(name))
            except:
                raise

        return messages

    # Invalid input type.
    else:
        msg = "The input 'names' must be a string or a list/tuple of strings."
        raise TypeError(msg)
