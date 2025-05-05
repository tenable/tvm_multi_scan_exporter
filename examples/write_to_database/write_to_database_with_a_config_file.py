import logging

from tvm_multi_scan_exporter import TvmMultiScanExporter

if __name__ == '__main__':
    """
    Set the following environment variables:
    1. TVM_ACCESS_KEY
    2. TVM_SECRET_KEY
    3. EXTERNAL_DB_PASSWORD
    """
    # set up logging
    logging.basicConfig(level=logging.INFO)

    # since a config file is provided in the same directory, there is no need t specify the properties in code.
    TvmMultiScanExporter().export()
