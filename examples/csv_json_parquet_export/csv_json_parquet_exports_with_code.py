from tvm_multi_scan_exporter import TvmMultiScanExporter, Config, Json, Csv, Parquet

if __name__ == '__main__':
    """
    Set the following environment variables:
    1. TVM_ACCESS_KEY
    2. TVM_SECRET_KEY
    """

    TvmMultiScanExporter().export(
        Config(
            scan_name="your-scan-name",
            allowed_memory_gb=4,
            export_type=Csv()  # or Parquet() or Json()
            # ... provide other options here
        )
    )
