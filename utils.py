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
import os
import glob

def globally_excluded(path):
  return os.path.basename(path) in ['Thumbs.db', ".SyncArchive", ".SyncID"] 

def human_readable_bytes(num):
  for x in [ 'b','KB','MB','GB' ]:
    if num < 1024.0 and num > -1024.0:
      return "%3.1f%s" % (num, x)
    num /= 1024.0
  return "%3.1f%s" % (num, 'TB')

def insensitive_glob(pattern):
	def either(c):
		return '[%s%s]'%(c.lower(),c.upper()) if c.isalpha() else c
	return glob.glob(''.join(map(either, pattern)))

def get_size(paths):
  total_size = 0
  for path in paths:
    if os.path.isdir(path):
      for dirpath, dirnames, filenames in os.walk(path):
        for f in filenames:
          fp = os.path.join(dirpath, f)
          total_size += os.path.getsize(fp)
    else:
      total_size += os.path.getsize(path)
  return total_size

def get_files_in_paths(paths):
  files = []
  for path in paths:
    if os.path.isdir(path):
      for p, dirnames, filenames in os.walk(path):
        for name in filenames:
          full_path = os.path.join(p, name)
          if not globally_excluded(full_path):
            files.append(full_path)
    else:
      files.append(path)
  files.sort()
  return files

