
# Python standard library imports
import os.path
import urllib.parse as urlparse
from abc import abstractmethod


class URLScheme(object):
    """
    Base class for all URL schemes. Where a scheme is a resource type such as ftp, s3, http, etc.
    This class should encapsulate the operations common to all schemes and define the required operations, yet ot be
    implemented.
    """

    TYPE = 'None'

    def __init__(self, url):
        """
        Constructor.

        Args:
            url: str - The absolute url path with any scheme, network location etc. defined.

        """
        self._url = url

    @classmethod
    def Validate(cls, url):
        """
        Check if a given url is of a scheme corrseponding to this class.

        Args:
            url: str - The absolute url path with any scheme, network location etc. defined.

        Return:
            bool - Is the given url applicable to this scheme.

        """
        parsed = urlparse.urlparse(url)
        if parsed.scheme.lower() == cls.TYPE:
            return True
        return False

    @abstractmethod
    def GetStream(self, permission):
        """
        Gets an io stream object, from which data can be pulled.

        Return:
            IOBase - A stream object for reading from.

        """
        raise NotImplementedError


class FileScheme(URLScheme):
    """
    A scheme defining operations relevant to local files.
    """

    TYPE = 'file'

    def GetStream(self, permission):
        """
        Gets an io stream object, from which data can be pulled.

        Return:
            IOBase - A stream object for reading from.

        """
        parsed = urlparse.urlparse(self._url)
        return open(parsed.path, permission)


_schemes = [
    FileScheme
]


def get_stream(url, permission):
    """
    Helper factory function - gets a read stream for a given URL.

    Args:
        url: str - The absolute url path with any scheme, network location etc. defined.

    Return:
            IOBase - A stream object for reading from.

    """
    # If no scheme in url, assume local file.
    parsed = urlparse.urlparse(url)
    if parsed.scheme == '':
        url = urlparse.urlunparse(('file', '', os.path.abspath(url), '', '', ''))

    # Parse URL.
    the_scheme = None
    for scheme in _schemes:
        if scheme.Validate(url):
            the_scheme = scheme(url)
            break

    # Return the stream for the url.
    return the_scheme.GetStream(permission)
