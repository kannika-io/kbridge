# Kannika Bridge

[![CI](https://github.com/cymo-eu/kannika-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/cymo-eu/kannika-bridge/actions/workflows/ci.yml)

Restore consumer offsets on kafka cluster.

## Install

### Linux

- Download binary
- Add binary to $PATH variable

## Usage

```bash
bridge-cli --bootstrap-server localhost:9093 --legacy-offset-header Offset --offsets-csv-file-location ./offsets.csv
```
