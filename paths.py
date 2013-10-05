# Copyright (c) 2013 Jacob Lewallen
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
#
import hashlib
import os

import utils

class Paths:
  def __init__(self, paths):
    self.paths = paths
    self._size = None
    self._files = None
    self._meta_hash = None
    self._hash = None

  def empty(self):
    return self.size() == 0

  def files(self):
		if self._files: return self._files
		self._files = utils.get_files_in_paths(self.paths)
		return self._files

  def meta_hash(self):
		if self._meta_hash:
			return self._meta_hash
		paths = self.files()
		def _update_checksum(checksum, path):
			(mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime) = os.stat(path)
			checksum.update(path)
			checksum.update("%s" % size)
			checksum.update("%s" % mtime)
		chksum = hashlib.sha1()
		for file in sorted(paths):
			_update_checksum(chksum, file)
		self._meta_hash = chksum.hexdigest()
		return self._meta_hash
		
  def sha1(self):
		if self._hash:
			return self._hash
		paths = self.files()
		def _update_checksum(checksum, path):
			fh = open(path, 'rb')
			while 1:
				buf = fh.read(4096)
				if not buf : break
				checksum.update(buf)
			fh.close()
		chksum = hashlib.sha1()
		for file in sorted(paths):
			_update_checksum(chksum, file)
		self._hash = chksum.hexdigest()
		return self._hash

  def size(self):
    if self._size:
      return self._size
    self._size = utils.get_size(self.files())
    return self._size

  def includes(self, path):
		if path in self.paths:
			return True
		for p in self.paths:
			if p.startswith(path):
				return True
		return path in self.files()

  def relative_dirs(self, top):
    return set([os.path.dirname(path).replace(top, '') for path in self.files()])


