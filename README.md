# dbc-to-ipynb

Convert Databricks DBC exports into Jupyter notebooks (.ipynb).

Features
- Minimal, readable code (single script).
- Handles JSON, base64, gzip, and zlib encoded DBC entries.
- Supports Databricks formats:
  - {"commands": [...]} (position-sorted)
  - {"cells": [...]} (Jupyter-like)
  - {"notebooks": [ ... ]} (multiple notebooks per entry)
- Recreates cells using Databricks "# COMMAND ----------" boundaries.

Install
```bash
pip install nbformat
```

Usage
- CLI:
  ```bash
  python converter.py path/to/file.dbc [out_dir]
  # default out_dir is "<dbc_basename>_ipynb"
  ```

- From code:
  ```python
  from converter import dbc_to_ipynbs
  ipynbs = dbc_to_ipynbs("dbc_file.dbc", "out_ipynbs")
  ```

Notes
- For non-Python notebooks, kernel metadata is set to the detected language where possible.
- If an entry can't be decoded as JSON, it is converted as a single-code-cell notebook with raw text.

License
MIT
