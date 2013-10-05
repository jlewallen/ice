#!/usr/bin/python

import os
import re
import hashlib
import tarfile
import tarfile
import tempfile
import beefish
import glob
import logging
import boto
import boto.s3
import json

from Crypto.Cipher import Blowfish
from Crypto import Random
from contextlib import closing

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
log = logging.getLogger('ice')

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

class Strategy(object):
	def __init__(self, top, path):
		self.top = top
		self.path = path

class SimpleStrategy(Strategy):
  def __init__(self, top, path):
    super(SimpleStrategy, self).__init__(top, path)

  def generate(self, archives):
    for path in insensitive_glob(self.path):
      if not archives.includes(path):
        archives.add(Archive(self.top, [path]))

class Batch(object):
  def __init__(self):
    self.paths = []
    self.size = 0

  def add(self, path, force=False):
    MAX_SIZE = 512 * 1024 * 1024
    directory_size = get_size([path])
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
    for path in insensitive_glob(self.path):
      batch = batch.add(path)
      batches.add(batch)
    for batch in batches:
			archives.add(Archive(self.top, batch.paths))

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

class WarnStrategy(Strategy):
	def __init__(self, top):
		super(WarnStrategy, self).__init__(top, top)
		self._files = None

	def files(self):
		if self._files: return self._files
		self._files = get_files_in_paths([self.top])
		return self._files
		
	def generate(self, archives):
		for path in self.files():
			if not archives.includes(path):
				log.warn(path)

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
		self._files = get_files_in_paths(self.paths)
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
    self._size = get_size(self.files())
    return self._size

  def includes(self, path):
		if path in self.paths:
			return True
		for p in self.paths:
			if p.startswith(path):
				return True
		return path in self.files()

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
  def __init__(self, top, paths):
		self.paths = Paths(paths)
		self.top = top

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
    return "Archive<%s, %s, %s, %s %s>" % (self.meta_hash(), len(self.paths.paths), len(self.files()), self.paths.paths[:1], human_readable_bytes(self.size()))

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
    self.meta['archives'][paths.sha1()] = archive_meta 
    s3.write_object_as_json(paths.sha1(), archive_meta)

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
    s3.write_object_as_json(self.name, {
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
    return s3.read_object_as_json(sha1)

  def get_sha1(self, meta_hash):
    return self.meta['meta_hashes'][meta_hash]

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

def upload_archive(vault, sha1, paths, upload=True, dryRun=False):
  files = paths.files()

  log.info("Have archive (sha1=%s)" % (sha1))
  with tempfile.NamedTemporaryFile(delete=False) as temp_fh:
    log.info("Making archive %s (%s)" % (sha1, temp_fh.name))
    if not dryRun:
      with closing(tarfile.open(fileobj=temp_fh, mode="w")) as tar:
        for file in files:
          tar.add(file, exclude=globally_excluded)
    encrypted_fh = tempfile.NamedTemporaryFile(delete=False)
    temp_fh.seek(0)
    log.info("Encrypting archive %s (%s)" % (sha1, encrypted_fh.name))
    if not dryRun:
      beefish.encrypt_file(temp_fh.name, encrypted_fh.name, BACKUP_PASSWORD)
      encrypted_fh.seek(0)
    if upload:
      log.info("Uploading archive %s (%s)" % (sha1, encrypted_fh.name))
      if not dryRun:
        archive_id = vault.concurrent_create_archive_from_file(encrypted_fh.name, sha1)
    os.remove(temp_fh.name)
    os.remove(encrypted_fh.name)

def discover(meta, archives):
  for archive in archives.archives: 
    if not meta.contains(archive.meta_hash()):
      log.info("Adding archive meta (%s)" % archive)
      meta.add(archive.paths)
      meta.write()

top = boto.config.get('Ice', 'top')
password = boto.config.get('Ice', 'password')
bucket_name = boto.config.get('Ice', 'bucket')
vault_name = boto.config.get('Ice', 'vault')

s3 = S3(password, bucket_name)
meta = Meta(s3)
meta.read()
archives = get_archive_set(top)
discover(meta, archives)
glacier = boto.connect_glacier()
vault = glacier.create_vault(vault_name)
for archive in archives.archives:
  meta_hash = archive.meta_hash()
  sha1 = meta.get_sha1(meta_hash)
  upload_archive(vault, sha1, archive.paths, upload=True, dryRun=True)
meta.write()

# EOF
