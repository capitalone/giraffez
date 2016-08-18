# -*- coding: utf-8 -*-

import os
import time
import hashlib
import base64

from Crypto.Cipher import AES

from ._compat import *


__all__ = ['create_key_file', 'generate_key', 'Crypto']


def create_key_file(path):
    iv = "{}{}".format(os.urandom(32), time.time())
    new_key = generate_key(ensure_bytes(iv))
    with open(path, "wb") as f:
        f.write(base64.b64encode(new_key))
    os.chmod(path, 0o400)

def generate_key(s):
    for i in range(65536):
        s = hashlib.sha256(s).digest()
    return s


class Crypto(object):
    def __init__(self, key, bs=16): 
        self.key = hashlib.sha256(key).digest()
        self.bs = bs

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:self.bs]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self.unpad(cipher.decrypt(enc[self.bs:])).decode("utf-8")

    def encrypt(self, raw):
        raw = self.pad(raw)
        iv = os.urandom(self.bs)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    def unpad(self, s):
        return s[:-ord(s[len(s)-1:])]

    @classmethod
    def from_key_file(cls, path):
        with open(path, "rb") as f:
            return cls(base64.b64decode(f.read()))
