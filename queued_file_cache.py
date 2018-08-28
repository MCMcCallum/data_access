"""
Created 05-26-18 by Matthew C. McCallum
"""

# Local imports
from .dir_funcs import get_filenames
from .url_stream import get_size
from .url_stream import copy_url

# Third party imports
# None.

# Python standard library imports
import os
import math
import shutil
import multiprocessing
import random
import pickle
import logging
import copy
import collections
import threading
import itertools
import time


logger = logging.getLogger(__name__)


class FStruct(object):
    """
    """
    def __init__(self, fname, size):
        """
        """
        self.size = size
        self.fname = fname


class QueuedFileCache(object):
    """
    This class is intended to take a location with a lot of URLs and move it chunk by chunk, incrementally, to a local
    directory.
    This is useful for situations where a system has a large but slow storage location (e.g., HDD or AWS S3) coupled with
    a smaller but faster local storage location, e.g., an SSD. In this way, a small chunk can be operated on at a fast speed, 
    while a background process moves over the next chunk for processing from the larger storage location.
    """

    _CACHE_METADATA_FNAME = ".cache.pkl"    # <= The filename to store the cache state in, in the caching directory.
    _CACHE_BLOCK_SIZE = 20                  # <= The number of files in each incremental cache block to copy over at any one time.

    def __init__(self, from_urls, to_dir, cache_size=None, increment_size=None):
        """
        Constructor.

        Args:
            from_urls: list(str) - A list of URLs from which to copy files to a local cache.

            to_dir: str - URL to the cache location which will be initially be filled with files up to size cache_size,
            and then incrementally added to up to cache_size + increment_size.
        
            cache_size: int - The maximum total size of files that are simultaneously accessible in the cache.

            increment_size: int - The maximum total size of files that may be prepared before they are officially added
            to the cache.
        """
        # TODO [matt.c.mccallum 08.25.18]: Make the cache directory if it does not already exist!

        if not cache_size:
            cache_size = 1024*1024*1024*1024*1024 # 1 petabyte should be bigger than this class ever has to deal with.
        if not increment_size:
            increment_size = cache_size + 1024*1024*1024 # 1 gigabyte of increment room by default.
        self._max_size = cache_size+increment_size
        self._cache_size = cache_size

        # Initialise member variables
        self._next_cache_update = collections.deque()
        self._current_cache = collections.deque()
        self._used_cache = collections.deque()
        self._uncached_files = collections.deque()

        self._pool = None
        self._next_cache_lock = threading.Lock()
        self._cache_dir = to_dir
        self._stop_signal = True
        self._currently_caching = False

        # Check if we already have the cache information saved
        metadata_filename = os.path.join(to_dir, self._CACHE_METADATA_FNAME)
        if os.path.exists(metadata_filename):

            # TODO [matthew.mccallum 05.26.18] There should be some checking for cache integrity here, e.g.:
            # - Files currently in the cache match the urls provided
            # - File sizes are correct
            # - Cache directories are correct

            with open(metadata_filename, 'rb') as metadata_file:
                metadata = pickle.load(metadata_file)
                self._current_cache = collections.deque(metadata['_current_cache'])
                self._used_cache = collections.deque(metadata['_used_cache'])
                self._uncached_files = collections.deque(metadata['_uncached_files'])
                self._next_cache_update = collections.deque(metadata['_next_cache_update'])

            # TODO [matt.c.mccallum 08.02.18]: Delete any files that are not in the current and next_cache_update.

        else:

            # Get all filenames and their sizes
            all_urls = copy.copy(from_urls)
            random.shuffle(all_urls)
            for filename in all_urls:
                fsize = get_size(filename)
                self._uncached_files.append(FStruct(filename, fsize))

            # Load the starting cache.
            this_size = 0
            while len(self._uncached_files) and (this_size + self._uncached_files[0].size < self._cache_size):
                filename = self._uncached_files[0]
                copy_url(filename.fname, self._local_fname(filename.fname, self._cache_dir))
                self._current_cache.append(self._uncached_files.popleft())
                this_size += self._current_cache[-1].size

        # Save state and prepare the next cache.
        self.SaveState()
        self.Start()

    def SaveState(self):
        """
        Saves the current state of the cache to file, in case it needs to be used next time - to prevent having 
        to prefill the cache a second time.
        """
        self._next_cache_lock.acquire()
        metadata = {
            '_current_cache': list(self._current_cache),
            '_used_cache': list(self._used_cache),
            '_uncached_files': list(self._uncached_files),
            '_next_cache_update': list(self._next_cache_update)
        }
        self._next_cache_lock.release()
        with open(os.path.join(self._cache_dir, self._CACHE_METADATA_FNAME), 'wb') as metadata_file:
            pickle.dump(metadata, metadata_file, pickle.HIGHEST_PROTOCOL)

    @property
    def current_files(self):
        """
        Type: list(str)
        Returns the local URLs for all files accessible in the current cache.
        """
        self._next_cache_lock.acquire()
        result = [self._local_fname(fstruct.fname, self._cache_dir) for fstruct in list(self._current_cache)]
        self._next_cache_lock.release()
        return result

    @property
    def size(self):
        """
        Type: int
        Returns the total size of all tracked files in the cache in bytes.
        """
        self._next_cache_lock.acquire()
        result = sum([item.size for item in list(self._current_cache)]) + sum([item.size for item in list(self._next_cache_update)])
        self._next_cache_lock.release()
        return result

    @property
    def active_size(self):
        """
        Type: int
        Returns the total size of all files that are currently accessible from the cache in bytes.
        """
        self._next_cache_lock.acquire()
        result = sum([item.size for item in list(self._current_cache)])
        self._next_cache_lock.release()
        return result

    @staticmethod
    def _local_fname(remote_fname, local_dir):
        """
        Helper method for converting a remote URL to one in a local folder.

        Args:
            remote_fname: str - The URL to a remote file.

            local_dir: str - The directory to move the URL to.

        Return:
            str - A URL with the remote URL's filename in the local directory.
        """
        return os.path.join(local_dir, os.path.basename(remote_fname))

    @staticmethod
    def _copy_set(file_list, directory):
        """
        A method that copies a list of files to a given directory.
        This is intended to be called asynchronously as a background process.

        Args:
            file_list: list(str) - A list of files, including complete paths, to be copied to the provided directory.

            directory: str - A URL path to copy the specified files to.

        Return:
            list_str - The URLs of all files that were copied over.
        """
        # Copy all files
        for filename in file_list:
            copy_url(filename, QueuedFileCache._local_fname(filename, directory))
        return file_list

    def _copy_callback(self, arg):
        """
        A callback to be called on cache copying completion or error. It will either raise the error locally, or update
        the caching state.

        Args:
            arg: list(str) or Exception: The return value from an asynchronous process. Either the list of URLs that were
            copied or an exception.
        """
        if isinstance(arg, Exception):
            self._stop_signal = True
            self._currently_caching = False
            raise arg
        elif arg:
            # If files were successfully cached, update the buffers, and start the next one.
            self._next_cache_lock.acquire()
            for i in range(len(arg)):
                assert arg[i] == self._uncached_files[0].fname    # <= Check we are only popping files that we expect to come back from the copy.
                self._next_cache_update.append(self._uncached_files.popleft())
            self._next_cache_lock.release()

            # Update the cache state on disk so we may continue next time.
            self.SaveState()

            # If noone has stopped the cache, keep on going.
            if not self._stop_signal:
                caching_thread = threading.Thread(target=self.PrepareNextCacheBlock)
                caching_thread.start()
            else:
                self._currently_caching = False
        else:
            self._stop_signal = True
            self._currently_caching = False
            raise Exception  # arg = False should indicate unsuccessful caching, but this does not currently occur.

    def PrepareNextCacheBlock(self):
        """
        Prepares the next cache which may be later switched to when it is ready.
        This preparation is performed asynchronously in a background process.
        This should be called in an asynchronous thread - If all cache memory is full, this function will block
        until there is space again.
        """
        logger.info("Peparing next cache block")
        self._pool = multiprocessing.Pool(1, maxtasksperchild=1)

        # Get the files to be cached
        self._next_cache_lock.acquire()
        new_files = copy.copy(list(itertools.islice(self._uncached_files, 0, self._CACHE_BLOCK_SIZE)))
        # If there aren't any new files start over with the previously used files
        if not len(new_files):
            # Check if there is any 'used_cache' before trying to replace it - we may be able to fit everything on one machine!
            if len(self._used_cache):
                self._uncached_files = self._used_cache
                self._used_cache = collections.deque()
                new_files = copy.copy(list(itertools.islice(self._uncached_files, 0, self._CACHE_BLOCK_SIZE)))
            else:
                new_files = []
        self._next_cache_lock.release()
        # If there are no new files from the 'used' or 'uncached' cache, there is nothing more to cache, so return.
        if not len(new_files):
            return

        # Check if the current cache size can fit the new block, otherwise poll for cache space.
        # NOTE [matt.c.mccallum 08.02.18]: Important to do this size checking outside of the cache lock
        #      block above to prevent deadlock.
        new_size = sum([item.size for item in new_files])
        while new_size + self.size > self._max_size:
            time.sleep(5)
            if self._stop_signal:
                self._currently_caching = False
                return

        # Set off an update in another process.
        new_files = [fstruct.fname for fstruct in new_files]
        self._pool.apply_async(self._copy_set,
                               (new_files, self._cache_dir),
                               callback=self._copy_callback,
                               error_callback=self._copy_callback)
        self._pool.close()

    def Stop(self):
        """
        Stop the caching process.
        """
        self._stop_signal = True

    def Start(self):
        """
        Start the caching process.
        """
        self._stop_signal = False
        self._currently_caching = True
        caching_thread = threading.Thread(target=self.PrepareNextCacheBlock)
        caching_thread.start()

    def IsCaching(self):
        """
        Whether this class is in the caching loop, either:
            a) Waiting for more space to continue caching
            b) Copying over files to the spare cache space
        """
        return self._currently_caching

    def Update(self):
        """
        This will switch over adding the new cache block to the current cache.
        """
        logger.info("Moving the next cache block on board.")

        # Add all the newly copied cache updates
        print("Adding " + str(len(self._next_cache_update)) + " files to cache.")
        self._next_cache_lock.acquire()
        for i in range(len(self._next_cache_update)):
            self._current_cache.append(self._next_cache_update.popleft())
        self._next_cache_lock.release()

        # Reduce the size of the cache by removing the oldest files until we are within the allowed cache size
        removed_file_count = 0
        while self.active_size > self._cache_size:
            removed_file_count += 1
            self._next_cache_lock.acquire()
            os.remove(self._local_fname(self._current_cache[0].fname, self._cache_dir))
            self._used_cache.append(self._current_cache.popleft())
            self._next_cache_lock.release()
        print("Removed " + str(removed_file_count) + " files from cache.")

        # We have just updated the cache. This is a good time to save state.
        self.SaveState()
