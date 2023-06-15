########
base64io
########

.. image:: https://img.shields.io/pypi/v/base64io.svg
   :target: https://pypi.python.org/pypi/base64io
   :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/base64io.svg
   :target: https://pypi.python.org/pypi/base64io
   :alt: Supported Python Versions

.. image:: https://img.shields.io/badge/code_style-black-000000.svg
   :target: https://github.com/ambv/black
   :alt: Code style: black

.. image:: https://readthedocs.org/projects/base64io-python/badge/
   :target: https://base64io-python.readthedocs.io/en/stable/
   :alt: Documentation Status

.. image:: https://travis-ci.org/aws/base64io-python.svg?branch=master
   :target: https://travis-ci.org/aws/base64io-python

.. image:: https://ci.appveyor.com/api/projects/status/ds8xvogp4m70j9ks?svg=true
   :target: https://ci.appveyor.com/project/mattsb42-aws/base64io-python-36722

This project is designed to develop a class, :class:`base64io.Base64IO`, that implements
a streaming interface for Base64 encoding.

Python has supported native Base64 encoding since version 2.4. However, there is no
streaming interface for Base64 encoding, and none is available from the community.

The legacy ``base64.encode`` and ``base64.decode`` interface lets you shuffle data between
two streams, but it assumes that you have two complete streams. We wanted a
standard stream that applies Base64 encoding and decoding.

:class:`base64io.Base64IO` provides an `io` streaming interface with context manager
support that transparently Base64-encodes data read from it. You can use it to transform
large files without caching the entire context in memory, or to transform an existing
stream.

For the latest full documentation, see `Read the Docs`_.

Find us on `GitHub`_.

***************
Getting Started
***************

:class:`base64io.Base64IO` has no dependencies other than the standard library and should
work with any version of Python greater than 2.6. We test it on CPython 2.6, 2.7, 3.3,
3.4, 3.5, 3.6, and 3.7.

Installation
============

.. code::

   $ pip install base64io

***
Use
***
:class:`base64io.Base64IO` wraps the input stream and transparently encodes or decodes
data written to or read from the input stream.

* ``write()`` encodes data before writing it to the wrapped stream
* ``read()`` decodes data after reading it from the wrapped stream

Because the position of the :class:`base64io.Base64IO` stream and the wrapped stream will
almost always be different, :class:`base64io.Base64IO` does not support:

* ``seek()``
* ``tell()``

Also, :class:`base64io.Base64IO` does not support:

* ``fileno()``
* ``truncate()``

Encode data
===========

.. warning::

   If you are not using :class:`base64io.Base64IO` as a context manager, when you write to
   a :class:`base64io.Base64IO` stream, you **must** close the stream after your final
   write. The Base64 transformation might hold up to two bytes of unencoded data in an
   internal buffer before writing it to the wrapped stream. Calling ``close()`` flushes
   this buffer and writes the padded result to the wrapped stream. The
   :class:`base64io.Base64IO` context manager does this for you.

.. code-block:: python

   from base64io import Base64IO

   with open("source_file", "rb") as source, open("encoded_file", "wb") as target:
       with Base64IO(target) as encoded_target:
           for line in source:
               encoded_target.write(line)

Decode data
===========

.. note::

   When it reads data from the wrapping stream, it might read up to three additional bytes
   from the underlying stream.

.. code-block:: python

   from base64io import Base64IO

   with open("encoded_file", "rb") as encoded_source, open("target_file", "wb") as target:
       with Base64IO(encoded_source) as source:
           for line in source:
               target.write(line)

*******
License
*******

This library is licensed under the Apache 2.0 License.

.. _Read the Docs: http://base64io-python.readthedocs.io/en/latest/
.. _GitHub: https://github.com/aws/base64io-python/
.. _base64 documentation: https://docs.python.org/3/library/base64.html#base64.decode
