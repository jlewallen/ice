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
import tarfile
import tempfile
import beefish
import os

from contextlib import closing

import ice

def upload_archive(vault, sha1, paths, upload=True, dryRun=False):
  files = paths.files()

  ice.log.info("Have archive (sha1=%s)" % (sha1))
  with tempfile.NamedTemporaryFile(delete=False) as temp_fh:
    ice.log.info("Making archive %s (%s)" % (sha1, temp_fh.name))
    if not dryRun:
      with closing(tarfile.open(fileobj=temp_fh, mode="w")) as tar:
        for file in files:
          tar.add(file, exclude=globally_excluded)
    encrypted_fh = tempfile.NamedTemporaryFile(delete=False)
    temp_fh.seek(0)
    ice.log.info("Encrypting archive %s (%s)" % (sha1, encrypted_fh.name))
    if not dryRun:
      beefish.encrypt_file(temp_fh.name, encrypted_fh.name, BACKUP_PASSWORD)
      encrypted_fh.seek(0)
    if upload:
      ice.log.info("Uploading archive %s (%s)" % (sha1, encrypted_fh.name))
      if not dryRun:
        archive_id = vault.concurrent_create_archive_from_file(encrypted_fh.name, sha1)
    os.remove(temp_fh.name)
    os.remove(encrypted_fh.name)

