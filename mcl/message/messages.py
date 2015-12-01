"""Object specification for creating messages in MCL.

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import json
import msgpack
import datetime
from sets import Set
from mcl.network.abstract import Connection as Connection


# Globally track Message() definitions. The meta-class _RegisterMeta() inserts
# Message() definitions into _MESSAGES when Message() objects are subclassed.
_MESSAGES = list()


class _MessageMeta(type):
    """Meta-class for manufacturing and globally registering Message() objects.

    The :py:class:`._MessageMeta` object is a meta-class designed to
    manufacture MCL :py:class:`.Message` classes. The meta-class works by
    dynamically adding mandatory attributes to a class definition at run time
    if and ONLY if the class inherits from :py:class:`.Connection`.

    Classes that inherit from :py:class:`.Message` must implement the
    ``mandatory`` and ``connection`` attributes where:

        - ``mandatory`` is a list of strings defining the names of mandatory
          message attributes that must be present when instances of the new
          :py:class:`.Message` objects are created. During instantiation the
          input list *args is mapped to the attributes defined by
          ``mandatory``. If ``mandatory`` is not present, a TypeError will be
          raised.

        - ``connection`` is an instance of a :py:class:`.Connection` object
          specifying where the message can be broadcast and received.

    The meta-class also maintains a global register of :py:class:`.Message`
    sub-classes. :py:class:`.Message` subclasses are added to the register when
    they are defined. During this process :py:class:`._MessageMeta` checks to
    see if a :py:class:`.Message` class with the same name has already been
    defined.

    Note that the list of :py:class:`.Message` sub-classes can be acquired by
    calling::

        messages = Message.__subclasses__()

    The reason the :py:class:`._MessageMeta` is preferred is that it can
    provide error checking at the time of definition. Note that sub-classes
    cannot easily be removed from the list returned by
    ``Message.__subclasses__()``. By using this meta-class,
    :py:class:`.Message` objects can be removed from the global register via
    other methods (see :py:func:`.remove_message_object`).

    Raises:
        TypeError: If a :py:class:`.Message` object with the same name already
            exists.
        TypeError: If the parent class is a :py:class:`.Message` object and
            ``mandatory`` is ill-specified.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a message class.

        Manufacture a Message class for objects inheriting from
        :py:class:`.Message`. This is done by searching the input dictionary
        ``dct`` for the keys ``mandatory`` and ``connection`` where:

            - ``mandatory`` is a list of strings defining the names of
              mandatory message attributes that must be present when instances
              of the new :py:class:`.Message` objects are created. During
              instantiation the input list *args is mapped to the attributes
              defined by ``mandatory``. If ``mandatory`` is not present, a
              TypeError will be raised.

            - ``connection`` is an instance of a :py:class:`.Connection` object
              specifying where the message can be broadcast and received.

        A new message class is manufactured using the definition specified by
        the attribute ``mandatory``. The property 'mandatory' is attached to
        the returned class.

        Args:
          cls (class): is the class being instantiated.
          name (string): is the name of the new class.
          bases (tuple): base classes of the new class.
          dct (dict): dictionary mapping the class attribute names to objects.

        Returns:
            :py:class:`.Message`: sub-class of :py:class:`.Message` with
                mandatory attributes defined by the original ``mandatory``
                attribute.

        Raises:
            NameError: If the ``name`` is message or a :py:class:`.Message`
                subclass with the same name already exists.
            TypeError: If the ``mandatory`` or ``connection`` attributes are
                ill-specified.
            ValueError: If the ``mandatory`` attribute contains the words
                `mandatory` or `connection`.

        """

        # Do not look for the mandatory attributes in the Message() base class.
        if (name == 'Message') and (bases == (dict,)):
            return super(_MessageMeta, cls).__new__(cls, name, bases, dct)

        # Do not look for the mandatory attributes in sub-classes of the
        # Message() base class.
        elif bases != (Message,):
            return super(_MessageMeta, cls).__new__(cls, name, bases, dct)

        # Cannot call messages 'Message'.
        if name == 'Message':
            raise NameError("Cannot name Message() subclasses 'Message'.")

        # Check that a message with the same name does not exist.
        elif name in [message.__name__ for message in _MESSAGES]:
            msg = "A Message() with the name '%s' already exists."
            raise NameError(msg % name)

        # Objects inheriting from Message() are required to have a 'mandatory'
        # and 'connection' attribute.
        mandatory = dct.get('mandatory', {})
        connection = dct.get('connection', None)

        # Ensure 'mandatory' is a list or tuple of strings.
        if ((not isinstance(mandatory, (list, tuple))) or
            (not all(isinstance(item, basestring) for item in mandatory))):
            msg = "'mandatory' must be a list or tuple or strings."
            raise TypeError(msg)

        # Ensure the connection object is properly specified.
        if not isinstance(connection, Connection):
            msg = "The argument 'connection' must be an instance of a "
            msg += "Connection() subclass."
            raise TypeError(msg)

        # Check that a message with the same connection does not exist.
        for message in _MESSAGES:
            if connection.to_dict() == message.connection.to_dict():
                msg = 'A Connection() with the same parameters already exists:'
                msg += ' %s' % str(connection)
                raise Exception(msg)

        # Detect duplicate attribute names.
        seen_attr = set()
        for attr in mandatory:
            if (attr == 'mandatory') or (attr == 'connection'):
                msg = "Field names cannot be 'mandatory' or 'connection'."
                raise ValueError(msg)
            if attr in seen_attr:
                raise ValueError('Encountered duplicate field name: %r' % attr)
            seen_attr.add(attr)

        # Add basic message attributes as read-only CLASS attributes. This is
        # done by dynamically manufacturing a meta-class with properties
        # returning the basic message attributes.
        metacls = type('%sMeta' % name, (cls,),
                       {'name': property(lambda cls: name),
                        'mandatory': property(lambda cls: mandatory),
                        'connection': property(lambda cls: connection)})

        # Add basic message attributes as read-only INSTANCE attributes. This
        # is done by adding properties that return the basic message attributes
        # to the manufactured class.
        del(dct['mandatory'])
        del(dct['connection'])
        dct['name'] = property(lambda cls: name)
        dct['mandatory'] = property(lambda cls: mandatory)
        dct['connection'] = property(lambda cls: connection)
        obj = super(_MessageMeta, cls).__new__(metacls, name, bases, dct)

        # Store message definition.
        _MESSAGES.append(obj)
        return obj


class Message(dict):
    """Base class for MCL message objects.

    The :py:class:`.Message` object provides a base class for defining MCL
    message objects. Objects inheriting from :py:class:`.Message` must
    implement the attribute ``mandatory`` where:

        - ``mandatory`` is a list of strings defining the names of mandatory
          connection parameters that must be present when instances of the new
          :py:class:`.Connection` object are created. If ``mandatory`` is not
          present, a TypeError will be raised.

    These attributes define a message format and allow :py:class:`.Message` to
    manufacture a message class adhering to the specified definition.

    Example usage::

        # Define new connection object.
        class ExampleMessage(Message):
            mandatory = ('A', 'B')

        # Instantiate empty object.
        example = ExampleMessage()
        print example

    Raises:
        TypeError: If any of the input argument are invalid.

    """
    __metaclass__ = _MessageMeta

    def __init__(self, *args, **kwargs):

        # If no inputs were passed into the constructor, initialise the object
        # with empty fields.
        if not args and not kwargs:
            empty = [None] * len(self.mandatory)
            kwargs = dict(zip(self.mandatory, empty))

        # Initialise message object with items.
        super(Message, self).__init__()
        self.update(*args, **kwargs)

        # Ensure the message adheres to specification.
        if not Set(self.keys()).issuperset(Set(self.mandatory)):
            msg = "'%s' must have the following items: [" % self['name']
            msg += ', '.join(self.mandatory)
            msg += '].'
            raise TypeError(msg)

    def __setitem__(self, key, value):
        """Set an item to a new value.

        Prevent write access to the keys 'name'.

        """

        # Prevent write access to Message name.
        if key == 'name' and key in self:
            msg = "The key value '%s' in '%s' is read-only."
            raise ValueError(msg % (key, self.__class__.__name__))

        # All other items can be accessed normally.
        else:
            super(Message, self).__setitem__(key, value)

    def __set_time(self):
        """Update the CPU time-stamp in milliseconds from UTC epoch.

        Note: This method should be platform independent and provide more
        precision than the time.time() method. See:

        https://docs.python.org/2/library/datetime.html#datetime.datetime.now

        """
        time_now = datetime.datetime.utcnow()
        time_origin = datetime.datetime.utcfromtimestamp(0)
        timestamp = (time_now - time_origin).total_seconds()
        super(Message, self).__setitem__('timestamp', timestamp)

    def to_json(self):
        """Return the contents of the message as a JSON string.

        Returns:
            str: JSON formatted representation of the message contents.

        """
        return json.dumps(self)

    def encode(self):
        """Return the contents of the message as serialised binary msgpack data.

        Returns:
            str: serialised binary msgpack representation of the message
                contents.

        """
        return msgpack.dumps(self)

    def __decode(self, data):
        """Unpack msgpack serialised binary data.

        Args:
            data (str): msgpack serialised message data.

        Returns:
            dict: unpacked message contents.

        Raises:
            TypeError: If the input binary data could not be unpacked.

        """

        try:
            dct = msgpack.loads(data)

            # The transmitted object is a dictionary.
            if type(dct) is dict:

                # Check if mandatory attributes are missing.
                missing = Set(self.mandatory) - Set(dct.keys())

                # Decode was successful.
                if not missing:
                    return dct

                # Transmitted object is missing mandatory fields.
                else:
                    msg = 'The transmitted object was missing the following '
                    msg += 'mandatory items: [' + ', '.join(missing) + '].'

            # Transmitted object was decoded but is not a dictionary.
            else:
                msg = "Serialised object is of type '%s' and not a dictionary."
                msg = msg % str(type(dct))

        # Decoding was unsuccessful.
        except Exception as e:
            msg = "Could not unpack message. Error encountered:\n\n%s" % str(e)

        # Raise error encountered during unpacking.
        raise TypeError(msg)

    def update(self, *args, **kwargs):
        """Update message contents with new values.

        Update message contents from an optional positional argument and/or a
        set of keyword arguments.

        If a positional argument is given and it is a serialised binary msgpack
        representation of the message contents, it is unpacked and used to
        update the contents of the message.

        If a positional argument is given and it is a mapping object, the
        message is updated with the same key-value pairs as the mapping object.

        If the positional argument is an iterable object. Each item in the
        iterable must itself be an iterable with exactly two objects. The first
        object of each item becomes a key in the new dictionary, and the second
        object the corresponding value. If a key occurs more than once, the
        last value for that key becomes the corresponding value in the message.

        If keyword arguments are given, the keyword arguments and their values
        are used to update the contents of the message

        To illustrate, the following examples all update the message in the
        same manner:

        If the key 'timestamp' is present in the input, the timestamp of the
        message is set to the input value. If no 'timestamp' value is
        specified, the CPU time-stamp, in milliseconds from UTC epoch, at the
        end of the update is recorded.

        Args:
            *args (list): positional arguments
            *jags (dict): keyword arguments.

        Returns:
            dict: unpacked message contents.

        Raises:
            TypeError: If the message contents could not be updated.

        """

        # Set the default timestamp to None. If it is updated by the passed in
        # arguments, we won't update it automatically.
        if 'timestamp' not in self:
            self['timestamp'] = None
        original_time = self['timestamp']

        if len(args) > 1:
            msg = 'Input argument must be a msgpack serialised dictionary, '
            msg += 'a mapping object or iterable object.'
            raise TypeError(msg)

        # Update message with a serialised dictionary:
        #
        #     msg.update(binary)
        #
        if (len(args) == 1) and (type(args[0]) is str):
            super(Message, self).update(self.__decode(args[0]))
            return

        # Update message with a dictionary (and keyword arguments):
        #
        #     msg.update(one=1, two=2, three=3)
        #     msg.update(zip(['one', 'two', 'three'], [1, 2, 3]))
        #     msg.update([('two', 2), ('one', 1), ('three', 3)])
        #     msg.update({'three': 3, 'one': 1, 'two': 2})
        #
        else:
            try:
                super(Message, self).update(*args, **kwargs)
            except Exception as e:
                msg = "Could not update message. Error encountered:\n\n%s"
                raise TypeError(msg % str(e))

        # Populate the name key with the message name.
        if 'name' not in self:
            super(Message, self).__setitem__('name', self.__class__.__name__)

        # The name parameter was modified.
        elif self['name'] != self.__class__.__name__:
            msg = "The key value '%s' in '%s' is read-only."
            raise ValueError(msg % ('name', self.__class__.__name__))

        # Record the time of update if the 'timestamp' field was not
        # specified. By checking for changes to the 'timestamp' field, users
        # can set null values (None) or falsy values (a timestamp of 0).
        if self['timestamp'] == original_time:
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


def list_messages(include=None, exclude=None):
    """List message objects derived from Message.

    Args:
        include (list): list of message object names to include.
        exclude (list): list of message object names to exclude.

    Returns:
        list: a list of message objects derived from :py:class:`.Message` is
            returned.

    """

    # Save includes.
    if isinstance(include, basestring):
        include = [include, ]
    elif include and not hasattr(include, '__iter__'):
        msg = "'include' must be a string or a list of strings.'"
        raise TypeError(msg)

    # Save excludes.
    if isinstance(exclude, basestring):
        exclude = [exclude, ]
    elif exclude and not hasattr(exclude, '__iter__'):
        msg = "'exclude' must be a string or a list of strings.'"
        raise TypeError(msg)

    # Filter available messages.
    messages = list()
    for message in _MESSAGES:

        # Do not include messages in the black list.
        if exclude and message.name in exclude:
            continue

        # Only include messages in the white list (if it exists).
        if include and message.name not in include:
            continue

        messages.append(message)

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
