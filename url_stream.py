"""
Created by Matt C. McCallum
"""


# Local imports
# None.

# Third party imports
import boto3

# Python standard library imports
import os.path
import urllib.parse as urlparse
from abc import abstractmethod
import io


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


class S3Scheme(URLScheme):
    """
    A scheme defining operations writing to or reading from s3.
    """

    TYPE = 's3'

    def GetStream(self, permission):
        """
        Gets an context manager type object that can be read from or written to depending
        on the permissions. This specifically will read from or write to an s3 location.

        Note: This is intended to be used exclusively with context managers.

        Args:
            permission - str - A permission specifying either binary reading from or writing
            to an s3 location. Currently only binary reads and writes are supported.
        """
        parsed = urlparse.urlparse(self._url)
        s3_key = parsed.path[1:]
        s3_bucket = parsed.netloc
        if permission == 'rb':
            s3 = boto3.client('s3')
            data = io.BytesIO()
            s3.download_fileobj(s3_bucket, s3_key, data)
            data.seek(0)
            return data
        elif permission == 'wb':
            print('Returning writer')
            return S3Writer(s3_bucket, s3_key)
        else:
            raise TypeError('Incorrect permission for s3 file')


class S3Writer(object):
    """
    A context managed S3 file handle type object that may be written to.

    Currently, the file is prepared in memory in a bytes IO stream and then uploaded
    to S3 on the exit of the context.
    This provides only one write call, but has its limitations due to RAM availability.
    """

    def __init__(self, bucket, key):
        """
        Constructor.
        
        Args:
            bucket -> str - Name of the AWS S3 Bucket.

            key -> str - Essentially the S3 path and filename.
        """
        self._s3_bucket = bucket
        self._s3_key = key
    
    def write(self, data):
        """
        Write to the in memory data buffer in preparation for writing to S3.
        Note this adheres to the file write interface and can be used in place
        of files, for example as an argument to pickle.dump(...).

        Args:
            data -> Bytes - A bytes object containing the data to write to S3.
        """
        self._data.write(data)

    def __enter__(self):
        """
        Start the context. This simply opens an empty byte stream and returns itself
        for writing to.

        Return:
            S3Writer - This object, for writing to.
        """
        self._data = io.BytesIO()
        return self

    def __exit__(self, *exc):
        """
        Close the context - That is, upload to S3.

        Args:
            exc -> tuple(type, value, traceback) - Any excpetion that occured during the
            context. Currently this is not handled in any way.
        """
        s3 = boto3.client('s3')
        self._data.seek(0)
        s3.upload_fileobj(self._data, self._s3_bucket, self._s3_key)


_schemes = [
    FileScheme,
    S3Scheme
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
