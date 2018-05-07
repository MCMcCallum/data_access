"""
Created 05-05-18 by Matt C. McCallum

A little module for inspecting directories.
"""


# Python standard library imports.
import os


def get_filenames(dirname, exts=['.wav', '.mp3']):
    """
    Gets all filenames with a given extension in a directory.

    Args:
        dirname: str - A string containing the path to a directory to be analyzed.

        exts: list(str) - A list of strings describing the extensions of all files to be returned.

    Return:
        list(str) - A list of paths to files that were found.
    """
    all_files = os.listdir(dirname)
    all_files = [fname for fname in all_files if os.path.splitext(fname)[1] in exts]
    all_files = [os.path.join(dirname, fname) for fname in all_files]
    return all_files
