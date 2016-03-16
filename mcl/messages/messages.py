"""Object specification for creating messages in MCL.

The :mod:`~.messages.messages` module provides a means for implementing MCL
message objects. This is done through the :class:`.Message` object.  Since
:class:`.Message` objects derive from python dictionaries, they operate near
identically.

:class:`.Message` objects are a specification of what structure of data is
being transmitted on a particular :class:`.abstract.Connection`. As a result
:class:`.Message` objects are defined by:

    - mandatory message attributes that must be present when instances of the
      new :class:`.Message` objects are created

    - a :class:`~.abstract.Connection` object instance specifying where the
      message can be broadcast and received

Creating MCL :class:`.Message` objects is simple and is demonstrated in the
following example:

.. testcode::

    from mcl import Message
    from mcl.network.udp import Connection as UdpConnection

    # Define a message.
    class ExampleMessage(Message):
        mandatory = ('text', )
        connection = UdpConnection('ff15::a')

    # Create instance of message.
    msg = ExampleMessage(text='hello world')
    print msg

    # Messages objects contain the a 'timestamp' key which records the UTC time
    # of when the message object was instantiated. To update the timestamp and
    # message attributes, use the update() method.
    msg.update(text="I'm a lumberjack")
    print msg

    # To update a message attribute without updating the timestamp, set it
    # directly.
    msg['text'] = 'Spam! Spam!'
    print msg

    # Serialise message into a msgpack binary string. Hex-ify the string for
    # demonstration and printability.
    print msg.encode().encode('hex')

    # The message can also be encoded as a JSON object.
    print msg.to_json()

.. testoutput::
   :hide:

   {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'hello world'}
   {'timestamp': ..., 'name': 'ExampleMessage', 'text': "I'm a lumberjack"}
   {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'Spam! Spam!'}
   ...

The following functions can be used to retrieve and manipulate
:class:`.Message` objects:

    - :func:`~.messages.get_message_objects` return :class:`.Message` object(s)
      from name(s)

    - :func:`~.messages.list_messages` list message objects derived from
      :class:`.Message`

    - :func:`~.messages.remove_message_object` de-register a :class:`.Message`
      object from the list of known messages

.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: James Ward <j.ward@acfr.usyd.edu.au>
.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import sets
import json
import time
import msgpack
import mcl.network.abstract


# Globally track Message() definitions. The meta-class _RegisterMeta() inserts
# Message() definitions into _MESSAGES when Message() objects are subclassed.
_MESSAGES = list()


class _MessageMeta(type):
    """Meta-class for manufacturing and globally registering Message() objects.

    The :class:`._MessageMeta` object is a meta-class designed to manufacture
    MCL :class:`.Message` classes. The meta-class works by dynamically adding
    mandatory attributes to a class definition at run time if and ONLY if the
    class inherits from :class:`.abstract.Connection`.

    Classes that inherit from :class:`.Message` must implement the `mandatory`
    and `connection` attributes where:

        - `mandatory` is a list of strings defining the names of mandatory
          message attributes that must be present when instances of the new
          :class:`.Message` objects are created. During instantiation the input
          list *args is mapped to the attributes defined by `mandatory`. If
          `mandatory` is not present, a :exc:`.TypeError` will be raised.

        - `connection` is an instance of a :class:`~.abstract.Connection`
          object specifying where the message can be broadcast and received.

    The meta-class also maintains a global register of :class:`.Message`
    sub-classes. :class:`.Message` sub-classes are added to the register when
    they are defined. During this process :class:`._MessageMeta` checks to see
    if a :class:`.Message` class with the same name has already been defined.

    Note that the list of :class:`.Message` sub-classes can be acquired by
    calling::

        messages = Message.__subclasses__()

    The reason the :class:`._MessageMeta` is preferred is that it can provide
    error checking at the time of definition. Note that sub-classes cannot
    easily be removed from the list returned by
    ``Message.__subclasses__()``. By using this meta-class, :class:`.Message`
    objects can be removed from the global register via other methods (see
    :func:`.remove_message_object`).

    Raises:
        TypeError: If a :class:`.Message` object with the same name already
            exists.
        TypeError: If the parent class is a :class:`.Message` object and
            `mandatory` is ill-specified.

    """

    def __new__(cls, name, bases, dct):
        """Manufacture a message class.

        Manufacture a Message class for objects inheriting from
        :class:`.Message`. This is done by searching the input dictionary `dct`
        for the keys `mandatory` and `connection` where:

            - `mandatory` is a list of strings defining the names of mandatory
              message attributes that must be present when instances of the new
              :class:`.Message` object are created. During instantiation the
              input list *args is mapped to the attributes defined by
              `mandatory`. If `mandatory` is not present, a :exc:`.TypeError`
              will be raised.

            - `connection` is an instance of a :class:`~.abstract.Connection`
              object specifying where the message can be broadcast and
              received.

        A new message class is manufactured using the definition specified by
        the attribute `mandatory`. The property 'mandatory' is attached to the
        returned class.

        Args:
          cls (class): is the class being instantiated.
          name (string): is the name of the new class.
          bases (tuple): base classes of the new class.
          dct (dict): dictionary mapping the class attribute names to objects.

        Returns:
            :class:`.Message`: sub-class of :class:`.Message` with mandatory
                attributes defined by the original `mandatory` attribute.

        Raises:
            NameError: If the `name` is message or a :class:`.Message` subclass
                with the same name already exists.
            TypeError: If the `mandatory` or `connection` attributes are
                ill-specified.
            ValueError: If the `mandatory` attribute contains the words
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
        if not isinstance(connection, mcl.network.abstract.Connection):
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

    The :class:`.Message` object provides a base class for defining MCL message
    objects. Objects inheriting from :class:`.Message` must implement the
    attribute `mandatory` where:

        - `mandatory` is a list of strings defining the names of mandatory
          connection parameters that must be present when instances of the new
          :class:`~.abstract.Connection` object are created. If `mandatory` is
          not present, a TypeError will be raised.

    These attributes define a message format and allow :class:`.Message` to
    manufacture a message class adhering to the specified definition.

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
        if not sets.Set(self.keys()).issuperset(sets.Set(self.mandatory)):
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

        """

        # Return the time in seconds since the epoch as a floating point
        # number. Note that even though the time is always returned as a
        # floating point number, not all systems provide time with a better
        # precision than 1 second. While this function normally returns
        # non-decreasing values, it can return a lower value than a previous
        # call if the system clock has been set back between the two calls.
        #
        # Note: The datetime documentation claims datetime.datetime.now()
        #       supplies more precision than can be gotten from time.time()
        #       timestamp if possible. To simplify the code
        #
        # From:
        #     https://docs.python.org/2/library/time.html#time.time
        #     https://docs.python.org/2/library/datetime.html#datetime.datetime.now
        #
        super(Message, self).__setitem__('timestamp', time.time())

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
                missing = sets.Set(self.mandatory) - sets.Set(dct.keys())

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

        .. testcode::

            serialised = ExampleMessage(text='hello world')
            print ExampleMessage(serialised)

        .. testoutput::
           :hide:

           {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'hello world'}

        If a positional argument is given and it is a mapping object, the
        message is updated with the same key-value pairs as the mapping object.

        .. testcode::

            print ExampleMessage({'text': 'hello world'})

        .. testoutput::
           :hide:

           {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'hello world'}

        If the positional argument is an iterable object. Each item in the
        iterable must itself be an iterable with exactly two objects. The first
        object of each item becomes a key in the new dictionary, and the second
        object the corresponding value. If a key occurs more than once, the
        last value for that key becomes the corresponding value in the message.

        .. testcode::

            print ExampleMessage(zip(('text',), ('hello world',)))

        .. testoutput::
           :hide:

           {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'hello world'}

        If keyword arguments are given, the keyword arguments and their values
        are used to update the contents of the message

        .. testcode::

            print ExampleMessage(text='hello world')

        .. testoutput::
           :hide:

           {'timestamp': ..., 'name': 'ExampleMessage', 'text': 'hello world'}

        If the key 'timestamp' is present in the input, the timestamp of the
        message is set to the input value. If no 'timestamp' value is
        specified, the CPU time-stamp, in milliseconds from UTC epoch, at the
        end of the update is recorded.

        Args:
            *args (list): positional arguments
            *kwargs (dict): keyword arguments.

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
            msg = "Attempted to set the read-only key value %s['%s'] = '%s'."
            raise ValueError(msg % (self.__class__.__name__,
                                    'name', self['name']))

        # Record the time of update if the 'timestamp' field was not
        # specified. By checking for changes to the 'timestamp' field, users
        # can set null values (None) or falsy values (a timestamp of 0).
        if self['timestamp'] == original_time:
            self.__set_time()


def remove_message_object(name):
    """De-register a :class:`.Message` object from the list of known messages.

    Args:
        name (string): Name of the :class:`.Message` object to de-register.

    Returns:
        bool: :data:`.True` if the :class:`.Message` object was
            de-registered. :data:`.False` if the :class:`.Message` object does
            not exist.

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
    """List objects derived from :class:`.Message`.

    Args:
        include (list): list of message object names to include.
        exclude (list): list of message object names to exclude.

    Returns:
        list: a list of message objects derived from :class:`.Message` is
            returned.

    """

    # Save includes.
    if isinstance(include, basestring):
        include = [include, ]
    elif include is not None:
        if ((not hasattr(include, '__iter__')) or
            (not all([isinstance(itm, basestring) for itm in include]))):
            msg = "'include' must be a string or a list of strings.'"
            raise TypeError(msg)

    # Save excludes.
    if isinstance(exclude, basestring):
        exclude = [exclude, ]
    elif exclude is not None:
        if ((not hasattr(exclude, '__iter__')) or
            (not all([isinstance(itm, basestring) for itm in exclude]))):
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
    """Return :class:`.Message` object(s) from name(s).

    Args:
        name (:obj:`python:string` or :obj:`python:list`): The name (as a
            string) of a single message object to retrieve. To retrieve
            multiple message objects, input a list containing the object names.

    Returns:
        Message or list: If a single message object is requested (string
            input), the requested py:class:`.Message` is returned. If multiple
            message objects are requested (list input), a list of message
            objects is returned.

    Raises:
        TypeError: If `names` is not a string or list/tuple of strings.
        NameError: If `names` does not exist or multiple message objects are
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
    elif ((isinstance(names, (list, tuple))) and
          (all([isinstance(itm, basestring) for itm in names]))):

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
