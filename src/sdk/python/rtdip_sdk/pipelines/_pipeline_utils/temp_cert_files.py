import os
from tempfile import NamedTemporaryFile


class TempCertFiles(object):
    """
    Allows to generate temporary certificate files and makes the files available for requests module to use.
    """

    def __init__(self, cert, key):
        self.cert = cert
        self.key = key

    def __enter__(self):
        with NamedTemporaryFile(mode="wb", delete=False) as cert_file:
            cert_file.write(self.cert)
            self.cert_file_name = cert_file.name

        with NamedTemporaryFile(mode="wb", delete=False) as key_file:
            key_file.write(self.key)
            self.key_file_name = key_file.name

        return self.cert_file_name, self.key_file_name

    def __exit__(self, exc_type, exc_value, traceback):
        os.remove(self.cert_file_name)
        os.remove(self.key_file_name)
