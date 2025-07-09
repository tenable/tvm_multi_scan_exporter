# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-05-28

### Added

- Initial release of the `tvm_multi_scan_exporter` library.- Support for exporting aggregated scan results in multiple formats: `CSV`, `JSON`, `Parquet`, and `WriteToDatabase`.
- `Config` class for configuring exports programmatically or via a `TOML` configuration file.
- `WriteToDatabase` export type with support for MS SQL Server, including batch writing and environment variable-based
  password configuration.
- Example usage for both programmatic and configuration file-based setups.
- Documentation for prerequisites, configuration options, and usage examples.

## [0.1.1] - 2025-07-09

### Fixed

- Fixed a bug that missed a few records in the final exported file.
