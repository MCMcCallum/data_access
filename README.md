
data_access
===

Written by Matt C. McCallum.

A Python module for dealing with urls, and the objects that occupy them.

Currently this module is limited to reading and writing numpy array based objects to local files, but it is its own
module as one day I'll likely need to add locations such as NFS, s3, etc., and data types, e.g., generic python objects,
TFRecords, etc. etc..

Dependencies
===

Just numpy for now...

Install
===

This is currently intended to be used as a git submodule:

`git submodule add https://github.com/MCMcCallum/data_access`

Alternatively, once I need the convenience I'll probably write a distutils `setup.py` for this module.