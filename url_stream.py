"""
Created by Matt C. McCallum
"""


# Local imports
# None.

# Third party imports
import boto3
from google.cloud import storage

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

    @abstractmethod
    def GetSize(self):
        """
        Gets the size of the file this scheme object refers to, in bytes.

        Return:
            int - The size of the file this url points to.
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

    def GetSize(self):
        """
        Gets the size of the file this scheme object refers to, in bytes.

        Return:
            int - The size of the file this url points to.
        """
        parsed = urlparse.urlparse(self._url)
        return os.path.getsize(parsed.path)
        

class S3Scheme(URLScheme):
    """
    A scheme defining operations writing to or reading from s3.
    """

    TYPE = 's3'

    def __init__(self, url):
        """
        Constructor.

        Args:
            url: str - The absolute url path with any scheme, network location etc. defined.
        """
        super().__init__(url)
        parsed = urlparse.urlparse(self._url)
        self._s3_key = parsed.path[1:]
        self._s3_bucket = parsed.netloc

    def GetStream(self, permission):
        """
        Gets an context manager type object that can be read from or written to depending
        on the permissions. This specifically will read from or write to an s3 location.

        Note: This is intended to be used exclusively with context managers.

        Args:
            permission - str - A permission specifying either binary reading from or writing
            to an s3 location. Currently only binary reads and writes are supported.
        """
        if permission == 'rb':
            s3 = boto3.client('s3')
            data = io.BytesIO()
            s3.download_fileobj(self._s3_bucket, self._s3_key, data)
            data.seek(0)
            return data
        elif permission == 'wb':
            return S3Writer(self._s3_bucket, self._s3_key)
        else:
            raise TypeError('Incorrect permission for s3 file')

    def GetSize(self):
        """
        Gets the size of the file this scheme object refers to, in bytes.

        Return:
            int - The size of the file this url points to.
        """
        s3 = boto3.client('s3')
        response = s3.head_object(Bucket=self._s3_bucket, Key=self._s3_key)
        return response['ContentLength']


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


class GSScheme(URLScheme):
    """
    A URLScheme defining operations for reading from or writing to Google Cloud Storage.
    """

    TYPE = "gs"

    def __init__(self, url):
        """
        Constructor.

        Args:
            url: str - The absolute url path with any scheme, network location etc. defined.
        """
        super().__init__(url)
        parsed = urlparse.urlparse(self._url)
        self._gs_name = parsed.path[1:]
        self._gs_bucket_name = parsed.netloc

    def _get_bucket(self, bucket_name):
        """
        Gets a GCS Bucket object for a given bucket name. If the bucket does not already exist in GCS,
        it is created.
        
        Args:
            bucket_name: str -> A string describing the bucket name.
        """
        storage_client = storage.Client()
        # Check if bucket exists first
        bucket = storage_client.lookup_bucket(bucket_name)
        # Create bucket if it doesn't exist
        if bucket is None:
            bucket = storage_client.bucket(bucket_name)
            bucket.storage_class = "REGIONAL"
            bucket.create(location='us-central1')
        return bucket

    def GetStream(self, permission):
        """
        Gets an context manager type object that can be read from or written to depending
        on the permissions. This specifically will read from or write to an s3 location.

        Note: This is intended to be used exclusively with context managers.

        Args:
            permission - str - A permission specifying either binary reading from or writing
            to an s3 location. Currently only binary reads and writes are supported.
        """
        self._bucket = self._get_bucket(self._gs_bucket_name)
        if permission == 'rb':
            blob = self._bucket.get_blob(self._gs_name)
            data = io.BytesIO()
            if blob:
                blob.download_to_file(data)
            data.seek(0)
            return data
        elif permission == 'wb':
            return GSWriter(self._bucket, self._gs_name)
        else:
            raise TypeError('Incorrect permission for s3 file')

    def GetSize(self):
        """
        Gets the size of the file this scheme object refers to, in bytes.

        Return:
            int - The size of the file this url points to.
        """
        raise NotImplementedError


class GSWriter(object):
    """
    A context managed Google Cloud Storage (GCS) file handle type object that may be written to.
    Thereby writing to a blob in GCS.

    Currently, the file is prepared in memory in a bytes IO stream and then uploaded
    to GCS on the exit of the context.
    This provides only one write call, but has its limitations due to RAM availability.
    """

    def __init__(self, bucket, name):
        """
        Constructor.
        
        Args:
            bucket -> str - Name of the AWS S3 Bucket.

            key -> str - Essentially the S3 path and filename.
        """
        self._gs_bucket = bucket
        self._gs_name = name
    
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
        self._data.seek(0)
        self._upload_blob(self._gs_bucket, self._data.getvalue(), self._gs_name)

    def _upload_blob(self, bucket, data, destination_blob_name):
        """
        Uploads a file to the provided GCS bucket under the provided blob name.

        Args:
            bucket: google.cloud.storage.bucket.Bucket -> A bucket to upload the data to.

            data: Bytes or String -> The data to be uploaded to GCS.

            destination_blob_name: String -> The name of the blob to store the data under
            in the GCS bucket.
        """
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data)


_schemes = [
    FileScheme,
    S3Scheme,
    GSScheme
]


def get_scheme(url):
    """
    Factory function for constructing schemes from URLs.

    Args:
        url -> str - The URL this stream refers to. If no scheme is specified a local file
        is assumed.
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

    return the_scheme


def get_stream(url, permission):
    """
    Helper factory function - gets a read stream for a given URL.

    Args:
        url -> str - The absolute url path with any scheme, network location etc. defined.

    Return:
        IOBase - A stream object for reading from or writing to.
    """
    the_scheme = get_scheme(url)
    return the_scheme.GetStream(permission)


def get_size(url):
    """
    Get the size of a file at a given URL.

    Args:
        url -> str - The URL to the file we want the size of.

    Return:
            int - The size of the file this url points to.
    """
    the_scheme = get_scheme(url)
    return the_scheme.GetSize()


def copy_url(from_url, dest_url):
    """
    Copy file from one url to another.

    Args:
        from_url -> str - The url of the file to copy from.

        dest_url -> str - The url to copy the file to.
    """
    with get_stream(from_url, 'rb') as from_stream:
        with get_stream(dest_url, 'wb') as to_stream:
            to_stream.write(from_stream.read())
