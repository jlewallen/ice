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
import boto.s3
import os
import json

from Crypto.Cipher import Blowfish
from Crypto import Random

class S3:
  def __init__(self, key, bucket_name):
    self.connection = boto.connect_s3()
    self.bucket = self.connection.create_bucket(bucket_name)
    self.key = key

  def decrypt_string(self, cipher, key):
    unpad = lambda s : s[0:-ord(s[-1])]
    c1  = Blowfish.new(key, Blowfish.MODE_ECB)
    return unpad(c1.decrypt(cipher))

  def encrypt_string(self, plain, key):
    pad = lambda s: s + (8 - len(s) % 8) * chr(8 - len(s) % 8) 
    c1  = Blowfish.new(key, Blowfish.MODE_ECB)
    return c1.encrypt(pad(plain))

  def read_object_as_json(self, key):
    k = boto.s3.key.Key(self.bucket)
    k.key = key
    decrypted = self.decrypt_string(k.get_contents_as_string(), key)
    return json.loads(decrypted.strip())

  def write_object_as_json(self, key, obj):
    serialized = json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
    encrypted = self.encrypt_string(serialized, key)
    k = boto.s3.key.Key(self.bucket)
    k.key = key
    k.set_contents_from_string(encrypted)
    if False:
      with open(key, "w") as f:
        f.write(serialized)

class Meta:
  def __init__(self, s3):
    self.name = "meta.json"
    self.path = os.path.join("meta", self.name)
    self.s3 = s3
    self.meta = {
      'meta_hashes': {},
      'archives': {}
    }
    if not os.path.isdir("meta"): os.mkdir("meta")

  def add(self, paths):
    archive_meta = {
      'size' : paths.size(),
      'meta_hash' : paths.meta_hash(),
      'files' : paths.files()
    }
    self.meta['meta_hashes'][paths.meta_hash()] = paths.sha1()
    s3.write_object_as_json(paths.sha1(), archive_meta)
    self.write()

  def to_json(self):
    return json.dumps(self.meta, sort_keys=True, indent=4, separators=(',', ': '))

  def from_json(self, json_string):
    self.meta = json.loads(json_string)

  def read(self):
    if os.path.exists(self.path):
      with open(self.path) as f:
        self.from_json(f.read())

  def write(self):
    with open(self.path, "w") as f:
      f.write(self.to_json())
    self.s3.write_object_as_json(self.name, {
      'meta_hashes' : self.meta['meta_hashes']
    })

  def contains(self, meta_hash):
     return meta_hash in self.meta['meta_hashes']

  def archives(self):
    sha1s = []
    meta_hashes = self.meta['meta_hashes']
    for meta_hash in meta_hashes.keys():
      sha1s.append(meta_hashes[meta_hash])
    return sha1s

  def get_archive_meta(self, sha1):
    path = os.path.join("meta", sha1)
    if os.path.exists(path):
      return json.loads(open(path).read())
    return self.s3.read_object_as_json(sha1)

  def get_sha1(self, meta_hash):
    return self.meta['meta_hashes'][meta_hash]

