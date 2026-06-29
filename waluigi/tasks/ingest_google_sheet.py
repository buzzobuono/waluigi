"""
IngestGoogleSheet — download a public Google Sheet and write it to the Catalog.

Works with sheets shared as "anyone with the link can view". Private sheets
require OAuth2 (not yet supported — see backlog).

config:
    input:
        spreadsheet_id: str         Google Sheets ID (from the URL)
        sheet_name:     str         optional — ingest only this sheet (default: all sheets)
        skip_rows:      int         rows to skip before the header row (default: 0)
        key_column:     str | int   column name or 0-based index used to filter null rows
                                    (default: 0 = first column; set to null to disable)
        add_sheet_column: bool      add a "sheet" column with the sheet name
                                    (default: true when multiple sheets, false when single)
        numeric_coerce: bool        auto-cast object columns to numeric where possible
                                    (default: true)
        numeric_coerce_exclude: list[str]  columns to skip during numeric coercion
    output:
        dataset:    str
        source_id:  str
        format:     str             (default: parquet)
        description: str
    force:  bool                    force new catalog version even if metadata unchanged
                                    (default: true)
"""
import io

import pandas as pd
import requests

from waluigi.sdk.catalog import catalog
from waluigi.sdk.context import context


def run():
    cfg            = context.config
    inp            = cfg.input
    spreadsheet_id = inp["spreadsheet_id"]
    single_sheet   = inp.get("sheet_name")
    skip_rows      = int(inp.get("skip_rows", 0))
    key_column     = inp.get("key_column", 0)       # str, int, or None
    add_sheet_col  = inp.get("add_sheet_column")    # None = auto
    numeric_coerce = inp.get("numeric_coerce", True)
    coerce_exclude = set(inp.get("numeric_coerce_exclude") or [])
    force          = cfg.get("force", True)

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=xlsx"
    print(f"  downloading spreadsheet {spreadsheet_id}...")
    resp = requests.get(url, timeout=60)
    if resp.status_code == 401:
        raise RuntimeError(
            "Sheet is private — share it as 'anyone with the link can view' "
            "or use an authenticated connector (OAuth2 not yet supported)"
        )
    resp.raise_for_status()

    excel_bytes = io.BytesIO(resp.content)
    all_sheets  = pd.ExcelFile(excel_bytes).sheet_names
    target_sheets = [single_sheet] if single_sheet else all_sheets
    print(f"  sheets in workbook: {all_sheets}")

    # auto add_sheet_column: true when ingesting multiple sheets
    if add_sheet_col is None:
        add_sheet_col = len(target_sheets) > 1

    frames = []
    for name in target_sheets:
        if name not in all_sheets:
            print(f"  skip '{name}': not found in workbook")
            continue

        df = pd.read_excel(excel_bytes, sheet_name=name,
                           header=skip_rows, engine="openpyxl")

        if df.empty:
            print(f"  skip '{name}': empty sheet")
            continue

        # filter rows where key_column is null
        if key_column is not None:
            col = df.columns[key_column] if isinstance(key_column, int) else key_column
            if col in df.columns:
                before = len(df)
                df = df[df[col].notna()].reset_index(drop=True)
                dropped = before - len(df)
                if dropped:
                    print(f"  '{name}': dropped {dropped} null-key rows (key_column='{col}')")

        if df.empty:
            print(f"  skip '{name}': no valid rows after null filter")
            continue

        if add_sheet_col:
            df.insert(0, "sheet", name)

        frames.append(df)
        print(f"  '{name}': {len(df)} rows, {len(df.columns)} columns")

    if not frames:
        raise RuntimeError("No sheets loaded — check sheet names and key_column filter")

    merged = pd.concat(frames, ignore_index=True)

    if numeric_coerce:
        exclude = coerce_exclude | ({"sheet"} if add_sheet_col else set())
        for col in merged.select_dtypes(include="object").columns:
            if col in exclude:
                continue
            converted = pd.to_numeric(merged[col], errors="coerce")
            if converted.notna().sum() > 0:
                merged[col] = converted

    print(f"  total: {len(merged)} rows, {len(frames)} sheet(s)")

    out = cfg.output
    source_id = out.get("source_id")
    if not source_id:
        raise ValueError(f"output.source_id is required (dataset: {out.get('dataset')})")

    handle = catalog.create_dataset(
        out["dataset"],
        format=out.get("format", "parquet"),
        source_id=source_id,
        description=out.get("description", f"Google Sheet {spreadsheet_id}"),
    )
    meta = {
        "spreadsheet_id": spreadsheet_id,
        "sheets":         len(frames),
        **vars(context.params),
    }
    with handle.create_version(metadata=meta, force=force) as writer:
        writer.write(merged)

    if writer.skipped:
        print(f"Skipped — same metadata: {writer.version}")
    else:
        print(f"Done: {writer.dataset_id} @ {writer.version} ({len(merged)} rows)")


if __name__ == "__main__":
    run()
