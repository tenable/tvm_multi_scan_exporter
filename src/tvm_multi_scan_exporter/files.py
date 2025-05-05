import atexit
import logging
import tempfile
from pathlib import Path


class TempDirectory:
    """
    Temporary container that destroys itself when the script ends
    """

    def __init__(self, prefix="tvm_multi_scan_exporter_"):
        self._tmp = tempfile.TemporaryDirectory(prefix=prefix)
        self.path = Path(self._tmp.name)

        # python's equivalent of a shutdown hook to clean up the temp directory
        atexit.register(self.cleanup)

    def cleanup(self):
        self._tmp.cleanup()
        logging.info(f"Cleaned up temporary folder at: {self.path}")
