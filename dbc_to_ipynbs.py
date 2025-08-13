import zipfile
import json
import base64
import gzip
import zlib
from pathlib import Path
from typing import List, Optional, Iterable, Tuple, Dict, Any

import nbformat as nbf

def _try_parse_json_from_bytes(b: bytes):
    """Try utf-8 JSON, base64->JSON, base64->gzip/zlib->JSON, gzip->JSON, zlib->JSON."""
    # utf-8 JSON
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        pass
    # base64 -> json
    try:
        decoded = base64.b64decode(b)
        try:
            return json.loads(decoded.decode("utf-8"))
        except Exception:
            pass
        try:
            return json.loads(gzip.decompress(decoded).decode("utf-8"))
        except Exception:
            pass
        try:
            return json.loads(zlib.decompress(decoded).decode("utf-8"))
        except Exception:
            pass
    except Exception:
        pass
    # gzip -> json
    try:
        return json.loads(gzip.decompress(b).decode("utf-8"))
    except Exception:
        pass
    # zlib -> json
    try:
        return json.loads(zlib.decompress(b).decode("utf-8"))
    except Exception:
        pass
    return None


def _normalize_lang(lang: Optional[str]) -> str:
    lang = (lang or "python").lower()
    if "py" in lang:
        return "python"
    if "sql" in lang:
        return "sql"
    if "scala" in lang:
        return "scala"
    if lang in ("r", "rscript"):
        return "r"
    return lang


def _iter_nb_objs(nbjson) -> Iterable[dict]:
    """
    Yield notebook dicts from:
      - {"commands": ...} or {"cells": ...}
      - {"notebooks": [ ... ]}
      - [ ... ]
      - dict fallback (emit raw dict)
    """
    if isinstance(nbjson, dict):
        if "commands" in nbjson or "cells" in nbjson:
            yield nbjson
            return
        nbs = nbjson.get("notebooks")
        if isinstance(nbs, list):
            for n in nbs:
                if isinstance(n, dict):
                    yield n
            return
        yield nbjson
        return
    if isinstance(nbjson, list):
        for item in nbjson:
            if isinstance(item, dict):
                yield item


def _extract_blocks(nb: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Return (name, text_with_separators, language) for a single notebook dict.
    """
    name = nb.get("name") or "notebook"
    lang = _normalize_lang(nb.get("language"))
    sep = "\n\n# COMMAND ----------\n\n"

    if "commands" in nb:
        commands = sorted(nb.get("commands", []), key=lambda c: c.get("position", 0))
        parts = []
        for cmd in commands:
            code = cmd.get("command", "")
            if isinstance(code, list):
                code = "".join(code)
            if isinstance(code, str) and code.strip():
                parts.append(code.strip())
        return name, sep.join(parts), lang

    if "cells" in nb:
        parts = []
        for c in nb.get("cells", []):
            src = c.get("command") if "command" in c else c.get("source", "")
            if isinstance(src, list):
                src = "".join(src)
            if isinstance(src, str) and src.strip():
                parts.append(src.strip())
        return name, sep.join(parts), lang

    # Fallback: dump as JSON
    try:
        return name, json.dumps(nb, indent=2), lang
    except Exception:
        return name, "", lang


def _write_ipynb(out_path: Path, text: str, lang: str) -> str:
    nb = nbf.v4.new_notebook()
    nb.metadata["kernelspec"] = {
        "display_name": "Python 3" if lang == "python" else lang,
        "language": lang,
        "name": "python3" if lang == "python" else lang
    }
    nb.metadata["language_info"] = {"name": lang}

    chunks = text.split("\n\n# COMMAND ----------\n\n")
    for ch in chunks:
        nb.cells.append(nbf.v4.new_code_cell(source=ch, metadata={}))

    out_path = _make_unique_path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        nbf.write(nb, f)
    return str(out_path)


def _make_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    i = 1
    while True:
        candidate = path.with_name(f"{path.stem}_{i}{path.suffix}")
        if not candidate.exists():
            return candidate
        i += 1


def dbc_to_ipynbs(dbc_path: str, out_dir: Optional[str] = None) -> List[str]:
    """
    Convert notebooks inside a Databricks .dbc (zip) into individual .ipynb files.
    """
    p = Path(dbc_path)
    if out_dir is None:
        out_dir = f"{p.with_suffix('')}_ipynb"
    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    if not zipfile.is_zipfile(dbc_path):
        raise ValueError(f"{dbc_path} is not a valid ZIP/DBC file")

    written: List[str] = []

    with zipfile.ZipFile(dbc_path, "r") as zf:
        for entry in zf.namelist():
            if entry.endswith("/") or entry.lower().endswith("manifest.mf"):
                continue
            raw = zf.read(entry)
            nbjson = _try_parse_json_from_bytes(raw)
            if nbjson is None:
                # fallback: text -> single-cell notebook
                try:
                    txt = raw.decode("utf-8", errors="replace")
                    if not txt.strip():
                        continue
                    name = Path(entry).stem
                    path = out_dir_p / f"{name}.ipynb"
                    written.append(_write_ipynb(path, txt, "python"))
                except Exception:
                    continue
                continue

            default_name = Path(entry).stem
            for nb in _iter_nb_objs(nbjson):
                name, text, lang = _extract_blocks(nb)
                name = name or default_name
                if not text.strip():
                    continue
                path = out_dir_p / f"{name}.ipynb"
                written.append(_write_ipynb(path, text, lang))

    return written


