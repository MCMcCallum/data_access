"""
Created 05-26-18 by Matthew C. McCallum
"""

# Local imports
from .dir_funcs import get_filenames

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


logger = logging.getLogger(__name__)


class FileCache(object):
    """
    This class is intended to take a location with a lot of URLs and move it chunk by chunk to a pair of local
    directories.
    This is useful for situations where a system has a large but slow storage location coupled with a smaller but faster
    local storage location, e.g., an SSD. In this way, a small chunk can be operated on at a fast speed, while a
    background process moves over the next chunk for processing from the larger storage location.
    """

    CACHE_METADATA_FNAME = ".cache.pkl"

    def __init__(self, from_dir, to_dir, size, ext):
        """
        Constructor.

        Args:
            from_dir: str - URL to the larger storage location that data with the provided extension will be cached
            from.

            to_dir: str - URL to the cache location which groups of files less than size will be copied over two. As the
            cache requires two groups at this location, this location must have storage space of at least 2*size.

            size: int - Size in bytes of each cache group. Two of these groups are copied to 'to_dir' simultaneously.

            ext: str - Extension of the files to be cached, including the '.' prefix.
        """
        # Initialise member variables
        self._pool = multiprocessing.Pool(1)
        self._cache_dir = to_dir
        self._currently_caching = False
        self._cache_a = None
        self._cache_b = None
        self._current_group = None
        self._current_cache = None
        self._all_files = None
        self._all_sizes = None
        self._cache_groups = None

        # Check if we already have the cache information saved
        metadata_filename = os.path.join(to_dir, self.CACHE_METADATA_FNAME)
        if os.path.exists(metadata_filename):

            # TODO [matthew.mccallum 05.26.18] There should be some checking for cache integrity here, e.g.:
            # - Files currently in the cache match one of the cache groups
            # - Cache groups match all filenames
            # - File sizes are correct
            # - Cache directories are correct

            with open(metadata_filename, 'rb') as metadata_file:
                metadata = pickle.load(metadata_file)
                self._all_files = metadata['_all_files']
                self._all_sizes = metadata['_all_sizes']
                self._cache_groups = metadata['_cache_groups']
                self._current_group = metadata['_current_group']
                self._current_cache = metadata['_current_cache']
                self._cache_a = metadata['_cache_a']
                self._cache_b = metadata['_cache_b']

        else:

            # Get all filenames and their sizes
            self._all_files = get_filenames(from_dir, ext)
            self._all_sizes = [0]*len(self._all_files)
            total_size = 0
            for ind, filename in enumerate(self._all_files):
                self._all_sizes[ind] = os.path.getsize(filename)
                total_size += self._all_sizes

            # Separate into cache groups.
            num_groups = int(math.ceil(total_size/size))
            size_per_group = total_size/num_groups
            filename_index = 0
            self._cache_groups = [[]]*num_groups
            for group in self._cache_groups:
                this_size = 0
                while this_size + self._all_sizes[filename_index] < size_per_group:
                    group += [self._all_files[filename_index]]
                    this_size += self._all_sizes[filename_index]
                random.shuffle(group)

            # Initialise member variables
            self._cache_a = os.path.join(self._cache_dir, 'a')
            self._cache_b = os.path.join(self._cache_dir, 'b')
            self._current_group = len(self._cache_groups)
            self._current_cache = self._cache_a

        # Prepare the next cache.
        self.PrepareNextCache()

    def __del__(self):
        """
        Destructor.

        Saves the current state of the cache for next time, to prevent having to prefill the cache a second time.
        """
        metadata = {
            '_all_files': self._all_files,
            '_all_sizes': self._all_sizes,
            '_cache_groups': self._cache_groups,
            '_current_group': self._current_group,
            '_current_cache': self._current_cache,
            '_cache_a': self._cache_a,
            '_cache_b': self._cache_b
        }
        with open(os.path.join(self._cache_dir, self.CACHE_METADATA_FNAME), 'rb') as metadata_file:
            pickle.dump(metadata, metadata_file, pickle.HIGHEST_PROTOCOL)

    @property
    def _next_group(self):
        """
        Gets the next group to be cached.

        Return:
            int - Index of the next group to be cached, or that is currently caching.
        """
        return (self._current_group + 1) % len(self._cache_groups)

    @staticmethod
    def _copy_set(file_list, directory):
        """
        A method that copies a list of files to a given directory. If that directory already exists, it will be cleared
        and replaced.
        This is intended to be called asynchronously as a background process.

        Args:
            file_list: str - A list of files, including complete paths, to be copied to the provided directory.

            directory: str - A URL location to copy the specified files to.

        Return:
            bool - Returns true upon successful completion.
        """
        # Clear directory first
        if os.path.exists(directory):
            shutil.rmtree(directory)
        os.mkdir(directory)

        # Copy all files
        for filename in file_list:
            shutil.copy(filename, directory)

        # Return success
        return True

    def _copy_callback(self, arg):
        """
        A callback to be called on cache copying completion or error. It will either raise the error locally, or update
        the caching state.

        Args:
            arg: bool or Exception: The return value from an asynchronous process. Either a success / fail value or an
            exception.
        """
        if issubclass(arg, Exception):
            self._currently_caching = False
            raise arg
        elif arg:
            self._currently_caching = False
        else:
            raise Exception  # arg = False should indicate unsuccessful caching, but this does not currently occur.

    def PrepareNextCache(self):
        """
        Prepares the next cache which may be later switched to when it is ready.
        This preparation is performed asynchronously in a background process.
        """
        logger.info("Peparing cache group at index: " + str(self._next_group))

        self._currently_caching = True
        # Copy all files to cache in another process
        if self._current_cache != self._cache_a: new_cache = self._cache_a
        else: new_cache = self._cache_b
        self._pool.apply_async(self._copy_set,
                               (self._cache_groups[self._next_group], new_cache),
                               callback=self._copy_callback,
                               error_callback=self._copy_callback)

    def IsCaching(self):
        """
        Returns True if there is currently a background process caching a set of files locally.
        """
        return self._currently_caching

    def SwitchCache(self):
        """
        This will switch over to the next cache, assuming it is ready, and start preparing the cache after that.
        """
        logger.info("Moving to next cache at index: " + str(self._next_group))

        if self.IsCaching():
            raise Exception  # Can't switch caches if we are currently preparing a cache.
        if self._current_cache != self._cache_a: self._current_cache = self._cache_a
        else: self._current_cache = self._cache_b
        self._currently_caching = True
        self._current_group = self._next_group
        self.PrepareNextCache()

