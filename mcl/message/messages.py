"""Object specification for creating messages in MCL.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import json
import msgpack
import datetime
from sets import Set


def _get_subclasses(cls):
    """Retrieve all derived objects from an input object.

    Args:
        cls (object): Object to inspect and retrieve derived objects.

    Returns:
        list: a list of objects and their children derived from ``cls``.

    """

    # WARNING: This may produce unwanted behaviour if subclasses have been
    #          created in factory methods or class definitions are created
    #          using closure. That is, the definition might be out of
    #          scope. The base class ``cls`` will permanently store all
    #          definitions and calls to cls.__subclassess__() will even return
    #          out-of-scope definitions.

    # Iterate through derived objects.
    subclasses = []
    for subclass in cls.__subclasses__():
        subclasses.append(subclass)

        # Recursively get derived objects from derived objects.
        subclasses.extend(_get_subclasses(subclass))

    return subclasses


def list_messages(name=False):
    """List message objects derived from Message.

    Args:

        name (boolean, **optional**): By default (``False``) a list of message
            objects derived from :py:class:`.Message` is returned. If set to
            ``True``, a list of tuples containing message objects derived from
            :py:class:`.Message` and their name as a string is returned.

    Returns:
        list: a list of message objects derived from :py:class:`.Message` is
            returned. a list of tuples containing message objects derived from
            :py:class:`.Message` and their name as a string is returned.

    """

    # Get all objects deriving from Message(). Do not include objects which
    # cannot be instantiated.
    messages = list()
    for message in _get_subclasses(Message):
        try:
            message()
            messages.append(message)
        except:
            pass

    # Get message names.
    if name:
        message_names = list()
        for message in messages:
            message_names.append(message.__name__)

        messages = zip(messages, message_names)

    # Return message objects.
    return messages


def get_message_object(name):
    """Return message object from name.

    Args:
        name (string): Name of message object to retrieve.

    Returns:
        :py:class:`.Message`: requested message object.

    Raises:
        Except: If ``name`` does not exist or multiple message objects are
            found.

    """

    # Get available messages.
    messages = list_messages(name=True)

    # Check if 'message' exists.
    message = list()
    for candiatate_message, candiatate_name in messages:
        if name == candiatate_name:
            message.append(candiatate_message)

    # Message does not exist.
    if len(message) == 0:
        raise Exception("Could locate the message named: '%s'." % name)

    # Multiple messages with the same name exist.
    elif len(message) > 1:
        msg = "Multiple messages named '%s' found including:\n" % name
        for m in message:
            msg += '    %s.%s\n' % (m.__module__, m.__name__)
        raise Exception(msg)

    # Return unique message.
    return message[0]


def get_message_objects(messages):
    """Return message object handles from names."""

    # Input is a string.
    if isinstance(messages, basestring):
        objects = get_message_object(messages)

    # Assume input is a list.
    else:
        objects = list()
        for message in messages:
            objects.append(get_message_object(message))

    return objects


class Message(dict):
    """Base class based on dict that is used for message passing.
    Has a set of mandatory fields that must be present.
    Additional fields will be serialised if present, but are not
    required."""

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
        """Ensure Message contains every entry in the 'mandatory_items'
        list.
        """
        if not Set(dct.keys()).issuperset(Set(self.__mandatory_items)):
            msg = "'%s' must have the following items: [" % self['name']
            msg += ', '.join(self.__mandatory_items)
            msg += '].'
            raise TypeError(msg)

    def __set_time(self):
        """Update the cpu timestamp in milliseconds from UTC epoch.

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
        Will not update message name or timestamp."""

        # Prevent write access to Message name and timestamp.
        if key == 'name' and key in self:
            msg = "The key value '%s' in '%s' is read-only."
            raise ValueError(msg % (key, self['name']))

        # All other items can be accessed normally.
        else:
            super(Message, self).__setitem__(key, value)

    def update(self, *args, **kwargs):
        """Update message with dictionary of new values.
        Automatically updates the timestamp."""
        # Set the default timestamp to None.
        # If it is updated by the passed in arguments, we won't update
        # it automatically.
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
