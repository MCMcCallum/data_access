"""
Created 05-05-18 by Matt C. McCallum

A little module for inspecting directories.
"""


# Python standard library imports.
import os


def get_audio_filenames(audio_dir, exts=['.wav', '.mp3']):
    """
    Gets all filenames with a given extension in a directory.

    Args:
        audio_dir: str - A string containing the path to a directory to be analyzed.

        exts: list(str) - A list of strings describing the extensions of all files to be returned.

    Return:
        list(str) - A list of paths to files that were found.
    """
    audio_files = os.listdir(audio_dir)
    audio_files = [fname for fname in audio_files if os.path.splitext(fname)[1] in exts]
    audio_files = [os.path.join(audio_dir, fname) for fname in audio_files]
    return audio_files
