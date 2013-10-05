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
import utils

from paths import *
import ice

class Strategy(object):
  def __init__(self, top, path):
    self.top = top
    self.path = path

  def __str__(self):
    return "Strategy<%s>" % (self.path)

class SimpleStrategy(Strategy):
  def __init__(self, top, path):
    super(SimpleStrategy, self).__init__(top, path)

  def generate(self, archives):
    for path in utils.insensitive_glob(self.path):
      if not archives.includes(path):
        archives.add(Archive(self, self.top, [path]))

class Batch(object):
  def __init__(self):
    self.paths = []
    self.size = 0

  def add(self, path, force=False):
    MAX_SIZE = 512 * 1024 * 1024
    directory_size = utils.get_size([path])
    if (self.size + directory_size) > MAX_SIZE and not force:
      new_batch = Batch()
      new_batch.add(path, True)
      return new_batch
    else:
      self.paths.append(path)
      self.size += directory_size
    return self

class BatchedStrategy(Strategy):
  def __init__(self, top, path):
    super(BatchedStrategy, self).__init__(top, path)

  def generate(self, archives):
    batch = Batch()
    batches = set()
    for path in utils.insensitive_glob(self.path):
      batch = batch.add(path)
      batches.add(batch)
    for batch in batches:
			archives.add(Archive(self, self.top, batch.paths))

class WarnStrategy(Strategy):
	def __init__(self, top):
		super(WarnStrategy, self).__init__(top, top)
		self._files = None

	def files(self):
		if self._files: return self._files
		self._files = utils.get_files_in_paths([self.top])
		return self._files
		
	def generate(self, archives):
		for path in self.files():
			if not archives.includes(path):
				ice.log.warn(path)

class ArchiveSet(object):
  def __init__(self):
    self.archives = []

  def includes(self, path):
    for archive in self.archives:
      if archive.includes(path):
				return True
    return False

  def add(self, archive):
    self.archives.append(archive)
	
class Archive(object):
  def __init__(self, strategy, top, paths):
    self.strategy = strategy
    self.top = top
    self.paths = Paths(paths)

  def includes(self, path):
    return self.paths.includes(path)

  def files(self):
    return self.paths.files()

  def sha1(self):
    return self.paths.sha1()

  def size(self):
    return self.paths.size()

  def meta_hash(self):
    return self.paths.meta_hash()
    
  def __str__(self):
    return "Archive<%s, %s, %s files, %s, %s>" % (self.meta_hash(), self.strategy, len(self.files()), self.paths.relative_dirs(self.top), utils.human_readable_bytes(self.size()))

