#!/usr/bin/python
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
import logging
import boto

import utils
import paths
import meta
import archiver

from archives import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
log = logging.getLogger('ice')

def get_archive_set(top):
  sources = [
    BatchedStrategy(top, os.path.join(top, "music/albums/*")),
    BatchedStrategy(top, os.path.join(top, "music/archived/*")),
    SimpleStrategy(top, os.path.join(top, "music/mixtapes/*")),
    SimpleStrategy(top, os.path.join(top, "music/singles")),
    SimpleStrategy(top, os.path.join(top, "music/downloaded")),
    SimpleStrategy(top, os.path.join(top, "pictures/library/*/*")),
    SimpleStrategy(top, os.path.join(top, "pictures/exported/*")),
    SimpleStrategy(top, os.path.join(top, "pictures/older/*")),
    SimpleStrategy(top, os.path.join(top, "pictures/*")),
    SimpleStrategy(top, os.path.join(top, "video/*")),
    SimpleStrategy(top, os.path.join(top, "scans")),
    #WarnStrategy(top)
  ]
  archives = ArchiveSet()
  for source in sources:
    source.generate(archives)
  return archives

def discover(meta, archives):
  for archive in archives.archives: 
    if not meta.contains(archive.meta_hash()):
      log.info("Adding archive meta (%s)" % archive)
      meta.add(archive.paths)

if __name__ == "__main__":
  top = boto.config.get('Ice', 'top')
  password = boto.config.get('Ice', 'password')
  bucket_name = boto.config.get('Ice', 'bucket')
  vault_name = boto.config.get('Ice', 'vault')

  s3 = meta.S3(password, bucket_name)
  meta = meta.Meta(s3)
  meta.read()
  archives = get_archive_set(top)
  discover(meta, archives)
  glacier = boto.connect_glacier()
  vault = glacier.create_vault(vault_name)
  for archive in archives.archives:
    log.info("Archive: %s" % archive)
    meta_hash = archive.meta_hash()
    sha1 = meta.get_sha1(meta_hash)
    archiver.upload_archive(vault, sha1, archive.paths, upload=True, dryRun=True)
  meta.write()

# EOF
