import logging

from tvm_multi_scan_exporter import TvmMultiScanExporter

if __name__ == '__main__':
    """
    """
    # set up logging
    logging.basicConfig(level=logging.INFO)

    # since a config file is provided, there is no need to specify the properties in code.
    TvmMultiScanExporter().export()
