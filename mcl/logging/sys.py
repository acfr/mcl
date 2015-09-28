"""Module for loggin MCL system activity.

.. sectionauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>
.. codeauthor:: Asher Bender <a.bender@acfr.usyd.edu.au>

"""
import os
import logging
from mcl import LOG_ROOT

# Initialise logging.
if os.path.exists(LOG_ROOT):

    # Create logger.
    LOGGER = logging.getLogger('MCL')
    LOGGER.setLevel(logging.INFO)

    # Set formatting of system logs. The message logs will look something like:
    #
    #     1970-01-01 00:00:11,111 [INFO]: message text
    #
    fmt = '%(asctime)s [%(levelname)s]: %(message)s'
    formatter = logging.Formatter(fmt)

    # Log system messages to file.
    fh = logging.FileHandler(os.path.join(LOG_ROOT, 'MCL.log'))
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    LOGGER.addHandler(fh)

    # Determine which messages will be logged.
    LOG_CRITICAL = LOGGER.isEnabledFor(logging.CRITICAL)
    LOG_ERROR = LOGGER.isEnabledFor(logging.ERROR)
    LOG_WARNING = LOGGER.isEnabledFor(logging.WARNING)
    LOG_INFO = LOGGER.isEnabledFor(logging.INFO)
    LOG_DEBUG = LOGGER.isEnabledFor(logging.DEBUG)

# Do not log system information.
else:
    LOG_CRITICAL = False
    LOG_ERROR = False
    LOG_WARNING = False
    LOG_INFO = False
    LOG_DEBUG = False


def format_syslog(cls, msg, *args):
    """Formant an MCL system log message."""

    if isinstance(cls, basestring):
        syslog_msg = '%s: ' % cls
    else:
        syslog_msg = '%s: ' % cls.__class__.__name__

    if args:
        syslog_msg += msg % args
    else:
        syslog_msg += msg

    return syslog_msg


def critical(cls, msg, *args):
    """Logs an MCL system message with level CRITICAL."""

    if LOG_CRITICAL:
        LOGGER.critical(format_syslog(cls, msg, *args))


def exception(cls, msg, *args):
    """Logs an MCL system message with level ERROR."""

    if LOG_ERROR:
        LOGGER.exception(format_syslog(cls, msg, *args))


def error(cls, msg, *args):
    """Logs an MCL system message with level ERROR."""

    if LOG_ERROR:
        LOGGER.error(format_syslog(cls, msg, *args))


def warning(cls, msg, *args):
    """Logs an MCL system message with level WARNING."""

    if LOG_WARNING:
        LOGGER.WARNING(format_syslog(cls, msg, *args))


def info(cls, msg, *args):
    """Logs an MCL system message with level INFO."""

    if LOG_INFO:
        LOGGER.info(format_syslog(cls, msg, *args))


def debug(cls, msg, *args):
    """Logs an MCL system message with level DEBUG."""

    if LOG_DEBUG:
        LOGGER.debug(format_syslog(cls, msg, *args))
