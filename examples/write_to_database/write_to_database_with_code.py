from tvm_multi_scan_exporter import TvmMultiScanExporter, Config, WriteToDatabase

if __name__ == '__main__':
    """
    Set the following environment variables:
    1. TVM_ACCESS_KEY
    2. TVM_SECRET_KEY
    3. EXTERNAL_DB_PASSWORD
    """

    TvmMultiScanExporter().export(
        Config(
            scan_name="your-scan-name",
            allowed_memory_gb=4,
            export_type=WriteToDatabase(
                user="your-db-user-name",
                host_address="your-db-server",
                port=1433,
                database_name="your-db-name",
                table_name="your_table_name"
                # ... provide other DB props here.
            )
            # ... provide other options here
        )
    )
