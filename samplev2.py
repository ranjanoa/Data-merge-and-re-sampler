#!/usr/bin/env python3
"""
merge_resample_service.py
Flask app to:
 - Merge files row-wise (Excel 'data' sheets + CSVs) in upload order.
 - Independently resample a chosen file to selected intervals using chunked aggregation.
 - Output merged/resampled CSVs using semicolon separator and comma decimals (numeric cells formatted to 3 dp).
 - Produce concise technical summaries of each operation (saved to outputs/*.txt and shown in the UI).
Usage:
  pip install flask pandas openpyxl
  python merge_resample_service.py
Open http://127.0.0.1:5000
"""
from flask import Flask, request, redirect, url_for, send_from_directory, render_template_string, flash
from werkzeug.utils import secure_filename
from pathlib import Path
import pandas as pd
import io, csv, re, math, datetime

app = Flask(__name__)
app.secret_key = "change_this_secret"
BASE = Path.cwd()
UPLOAD_DIR = BASE / "uploads"
OUT_DIR = BASE / "outputs"
for d in (UPLOAD_DIR, OUT_DIR):
    d.mkdir(parents=True, exist_ok=True)
ALLOWED_EXT = {'.csv', '.xls', '.xlsx'}
NUM_COMMA_RE = re.compile(r'^[+-]?\d{1,3}(?:\.\d{3})*,\d+$|^[+-]?\d+,\d+$')

INDEX_HTML = """
<!doctype html>
<title>Merge & Independent Resample Service</title>
<h2>1) Merge files (row-wise)</h2>
<form method=post enctype=multipart/form-data action="{{ url_for('merge') }}">
  <p>Data files (select multiple, in merge order): <input type=file name=data_files multiple required></p>
  <p>Optional LEGEND.csv (first non-empty line used as header): <input type=file name=legend_file></p>
  <p><input type=submit value="Merge and Produce merged_data.csv"></p>
</form>
<hr>
<h2>2) Resample a file independently</h2>
<p>You can either choose an existing file from the outputs folder, or upload a file to resample.</p>
<form method=post enctype=multipart/form-data action="{{ url_for('resample') }}">
  <p>Resample interval:
     <select name="resample">
       <option value="1T">1 minute</option>
       <option value="5T">5 minutes</option>
       <option value="15T">15 minutes</option>
       <option value="30T">30 minutes</option>
       <option value="H">1 hour</option>
     </select>
  </p>
  <p>Chunksize for processing (rows per chunk, larger = faster but more memory): <input name="chunksize" value="200000" /></p>

  <h4>Select existing outputs file</h4>
  <p>
    <select name="choose_existing">
      <option value="">-- choose existing file (optional) --</option>
      {% for f in outputs %}
        <option value="{{ f }}">{{ f }}</option>
      {% endfor %}
    </select>
  </p>

  <h4>Or upload a file to resample</h4>
  <p><input type=file name=resample_file></p>

  <p><input type=submit value="Run Resample -> outputs/merged_resampled.csv"></p>
</form>
{% with messages = get_flashed_messages() %}
  {% if messages %}
    <ul style="color:green;">{% for m in messages %}<li>{{ m }}</li>{% endfor %}</ul>
  {% endif %}
{% endwith %}
<hr>
<p>Download merged: <a href="{{ url_for('download', filename='merged_data.csv') }}">merged_data.csv</a></p>
<p>Download resampled: <a href="{{ url_for('download', filename='merged_resampled.csv') }}">merged_resampled.csv</a></p>
<p>Download merge summary: <a href="{{ url_for('download', filename='merge_summary.txt') }}">merge_summary.txt</a></p>
<p>Download resample summary: <a href="{{ url_for('download', filename='resample_summary.txt') }}">resample_summary.txt</a></p>
"""

def read_excel_data_sheet_bytes(file_bytes, filename):
    """Read first sheet named 'data' from Excel bytes (case-insensitive)."""
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as e:
        raise RuntimeError(f"Cannot open Excel {filename}: {e}")
    data_sheets = [s for s in xls.sheet_names if s.lower() == 'data']
    if not data_sheets:
        return []
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=data_sheets[0], header=None, dtype=str)
    df = df.fillna('')
    return df.astype(str).values.tolist()

def read_csv_rows_bytes(file_bytes, filename):
    """Detect delimiter and return rows as lists of strings."""
    text = None
    for enc in ('utf-8','latin-1','cp1252'):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            text = None
    if text is None:
        raise RuntimeError(f"Cannot decode CSV {filename}")
    sample = text[:8192]
    try:
        delim = csv.Sniffer().sniff(sample, delimiters=[',',';']).delimiter
    except Exception:
        delim = ';' if sample.count(';') > sample.count(',') else ','
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    return [list(map(lambda c: '' if c is None else c, r)) for r in reader]

def read_legend_header_tokens(file_bytes):
    text = None
    for enc in ('utf-8','latin-1','cp1252'):
        try:
            text = file_bytes.decode(enc)
            break
        except Exception:
            text = None
    if not text:
        return None
    # detect delimiter for legend header like CSV detection
    first_line = None
    for line in text.splitlines():
        if line.strip():
            first_line = line
            break
    if first_line is None:
        return None
    try:
        delim = csv.Sniffer().sniff(first_line, delimiters=[',',';']).delimiter
    except Exception:
        delim = ';' if first_line.count(';') > first_line.count(',') else ','
    return [c.strip() for c in first_line.split(delim)]

def is_header_like(row, legend_tokens):
    if not legend_tokens or not row:
        return False
    row_l = [str(c).strip().lower() for c in row]
    hdr_l = [t.strip().lower() for t in legend_tokens]
    matches = sum(1 for cell in row_l if cell in hdr_l)
    return matches >= max(2, len(hdr_l)//3)

def format_cell_3dp_comma(cell):
    """Format numeric-like strings to 3 decimals with comma; else return trimmed string."""
    s = '' if cell is None else str(cell).strip()
    if s == '':
        return ''
    try:
        # handle comma-decimal with thousands dots
        if NUM_COMMA_RE.match(s):
            normalized = s.replace('.', '').replace(',', '.')
            val = float(normalized)
            return f"{val:.3f}".replace('.', ',')
        if ',' in s:
            val = float(s.replace(',', '.'))
            return f"{val:.3f}".replace('.', ',')
        val = float(s)
        return f"{val:.3f}".replace('.', ',')
    except Exception:
        return s

@app.route('/', methods=['GET'])
def index():
    # list files in outputs to populate the select box
    outputs = [p.name for p in OUT_DIR.iterdir() if p.is_file() and p.suffix.lower() in ALLOWED_EXT.union({'.txt'})]
    outputs = sorted(outputs)
    return render_template_string(INDEX_HTML, outputs=outputs)

@app.route('/merge', methods=['POST'])
def merge():
    files = request.files.getlist('data_files')
    legend_file = request.files.get('legend_file')
    if not files:
        flash("No data files provided.")
        return redirect(url_for('index'))
    legend_tokens = None
    if legend_file and legend_file.filename:
        try:
            legend_bytes = legend_file.read()
            legend_tokens = read_legend_header_tokens(legend_bytes)
        except Exception as e:
            flash(f"Could not read legend header: {e}")
    merged_rows = []
    skipped = []
    file_row_counts = {}  # filename -> rows read
    files_processed = []
    for f in files:
        fname = secure_filename(f.filename)
        ext = Path(fname).suffix.lower()
        if ext not in ALLOWED_EXT:
            skipped.append((fname, 'unsupported'))
            continue
        data = f.read()
        try:
            if ext in ('.xls', '.xlsx'):
                rows = read_excel_data_sheet_bytes(data, fname)
                if not rows:
                    skipped.append((fname, "no 'data' sheet"))
                    continue
            else:
                rows = read_csv_rows_bytes(data, fname)
            file_row_counts[fname] = len(rows)
            files_processed.append(fname)
            if legend_tokens and rows and is_header_like(rows[0], legend_tokens):
                rows = rows[1:]
                # adjust recorded count to reflect removed header row for clarity
                file_row_counts[fname] = max(0, file_row_counts[fname] - 1)
            merged_rows.extend(rows)
        except Exception as e:
            skipped.append((fname, f"error: {e}"))
            continue
    if not merged_rows:
        flash("No rows merged. Check input files.")
        return redirect(url_for('index'))
    max_cols = max(len(r) for r in merged_rows)
    normalized = [r + ['']*(max_cols - len(r)) for r in merged_rows]
    if legend_tokens:
        header = legend_tokens[:max_cols]
        if len(header) < max_cols:
            header += [f'col_{i}' for i in range(len(header)+1, max_cols+1)]
    else:
        header = ['timestamp'] + [f'col_{i}' for i in range(2, max_cols+1)]
    out_path = OUT_DIR / 'merged_data.csv'
    # write semicolon delimited; timestamp preserved; numeric cells formatted to 3dp comma
    with out_path.open('w', encoding='utf-8', newline='') as fout:
        writer = csv.writer(fout, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        for row in normalized:
            if len(row) < max_cols:
                row = row + [''] * (max_cols - len(row))
            elif len(row) > max_cols:
                row = row[:max_cols]
            ts = '' if row[0] is None else str(row[0]).replace('\r',' ').replace('\n',' ').strip()
            rest = [format_cell_3dp_comma(cell) for cell in row[1:]]
            writer.writerow([ts] + rest)

    # Summary statistics for merge
    total_input_rows = sum(file_row_counts.values())
    output_rows = len(normalized)
    cols = max_cols
    total_output_cells = output_rows * cols
    non_empty_output_cells = sum(1 for r in normalized for c in r if str(c).strip() != '')
    sparsity_pct = 100.0 * (1 - (non_empty_output_cells / total_output_cells)) if total_output_cells else 0.0

    now = datetime.datetime.utcnow().isoformat() + "Z"
    summary_lines = [
        f"Merge operation summary",
        f"Timestamp (UTC): {now}",
        f"Files provided: {len(files)}",
        f"Files processed: {len(files_processed)} ({', '.join(files_processed)})" if files_processed else "Files processed: 0",
        f"Files skipped: {len(skipped)}" + (": " + ", ".join(f"{s[0]} ({s[1]})" for s in skipped) if skipped else ""),
        f"Total input rows (sum of source file rows, excluding detected legend/header lines): {total_input_rows}",
        f"Rows written to merged_data.csv: {output_rows}",
        f"Columns (max columns found across inputs): {cols}",
        f"Total output cells: {total_output_cells}",
        f"Non-empty output cells: {non_empty_output_cells} ({100.0 * non_empty_output_cells/total_output_cells:.2f}% filled)",
        f"Sparsity (empty cells): {sparsity_pct:.2f}%",
        f"Header used: {', '.join(header)}",
    ]
    summary_text = "\n".join(summary_lines)
    # save summary file
    merge_summary_path = OUT_DIR / 'merge_summary.txt'
    with merge_summary_path.open('w', encoding='utf-8') as sf:
        sf.write(summary_text + "\n")

    flash(f"Merged {output_rows} rows to {out_path.name}. Non-empty cells: {non_empty_output_cells} ({sparsity_pct:.2f}% empty). Summary saved to {merge_summary_path.name}")
    return redirect(url_for('index'))

@app.route('/resample', methods=['POST'])
def resample():
    # resample an existing outputs file OR an uploaded file -> merged_resampled.csv
    resample_rule = request.form.get('resample', '').strip()
    try:
        chunksize = int(request.form.get('chunksize', '200000'))
    except Exception:
        chunksize = 200000

    # Priority: uploaded file -> selected existing -> default outputs/merged_data.csv
    uploaded = request.files.get('resample_file')
    chosen = request.form.get('choose_existing', '').strip()
    temp_path = None

    # Use uploaded file if provided
    if uploaded and uploaded.filename:
        ufname = secure_filename(uploaded.filename)
        temp_path = UPLOAD_DIR / ufname
        try:
            uploaded.save(temp_path)
        except Exception as e:
            flash(f"Could not save uploaded resample file: {e}")
            return redirect(url_for('index'))
        merged_path = temp_path
    elif chosen:
        merged_path = OUT_DIR / chosen
        if not merged_path.exists():
            flash(f"Chosen file {chosen} not found in outputs.")
            return redirect(url_for('index'))
        if merged_path.suffix.lower() not in ALLOWED_EXT:
            flash(f"Chosen file {chosen} has unsupported extension.")
            return redirect(url_for('index'))
    else:
        merged_path = OUT_DIR / 'merged_data.csv'
        if not merged_path.exists():
            flash("merged_data.csv not found — run Merge first or provide a file.")
            return redirect(url_for('index'))

    # read header to find timestamp column name
    with merged_path.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.reader(fh, delimiter=';')
        try:
            header = next(reader)
        except StopIteration:
            if temp_path and temp_path.exists():
                temp_path.unlink(missing_ok=True)
            flash("Selected file is empty.")
            return redirect(url_for('index'))
    ts_col = header[0]
    if not resample_rule:
        if temp_path and temp_path.exists():
            temp_path.unlink(missing_ok=True)
        flash("No resample rule provided.")
        return redirect(url_for('index'))

    # detect numeric columns by reading a sample (strings)
    sample = pd.read_csv(merged_path, sep=';', decimal=',', nrows=1000, dtype=str)
    numeric_cols = []
    for c in sample.columns[1:]:
        s = sample[c].astype(str).str.replace(',', '.', regex=False)
        coerced = pd.to_numeric(s, errors='coerce')
        if coerced.notna().sum() > 0:
            numeric_cols.append(c)

    # Prepare accumulators for numeric sum and count per period
    sums_df = None
    counts_df = None
    firsts = {}  # dict period -> dict of non-numeric first values

    # Counters for summary
    rows_read_total = 0           # total rows read (all chunks)
    rows_with_valid_ts = 0        # rows kept after timestamp parsing
    # Process in chunks for memory efficiency
    parse_dates = [ts_col]
    reader = pd.read_csv(merged_path, sep=';', decimal=',', header=0, parse_dates=parse_dates,
                         dayfirst=False, chunksize=chunksize, dtype=str)
    for chunk in reader:
        rows_read_total += len(chunk)
        # ensure timestamp parsed
        chunk[ts_col] = pd.to_datetime(chunk[ts_col], errors='coerce')
        chunk = chunk.dropna(subset=[ts_col])
        rows_with_valid_ts += len(chunk)
        if chunk.empty:
            continue
        # floor timestamps to period
        chunk['_period'] = chunk[ts_col].dt.floor(resample_rule)
        # numeric conversion for numeric_cols (only if any)
        if numeric_cols:
            for c in numeric_cols:
                # replace comma decimals in strings and convert to float
                chunk[c] = pd.to_numeric(chunk[c].astype(str).str.replace(',', '.', regex=False), errors='coerce')
            # aggregate numeric sums/counts per period
            num_group = chunk.groupby('_period')[numeric_cols].agg(['sum', 'count'])
            # xs to get sums and counts
            sums_chunk = num_group.xs('sum', level=1, axis=1)
            counts_chunk = num_group.xs('count', level=1, axis=1)
            if sums_df is None:
                sums_df = sums_chunk
                counts_df = counts_chunk
            else:
                sums_df = sums_df.add(sums_chunk, fill_value=0)
                counts_df = counts_df.add(counts_chunk, fill_value=0)
        # handle non-numeric columns: store first non-empty value per period if not already present
        non_numeric_cols = [c for c in chunk.columns if c not in (numeric_cols + [ts_col, '_period'])]
        if non_numeric_cols:
            grouped_first = chunk.groupby('_period')[non_numeric_cols].first()
            for period, row in grouped_first.iterrows():
                key = pd.Timestamp(period)
                if key not in firsts:
                    firsts[key] = {}
                for c in non_numeric_cols:
                    v = row.get(c, '')
                    if pd.isna(v):
                        v = ''
                    existing = firsts[key].get(c, '')
                    # keep the first non-empty value seen
                    if existing == '' and v != '':
                        firsts[key][c] = v

    # If no numeric data found (sums_df None), proceed with non-numeric only
    if sums_df is not None:
        mean_df = sums_df.divide(counts_df.replace(0, pd.NA))
        mean_df = mean_df.reset_index()
        # rename period column to timestamp column name if needed
        if mean_df.columns[0] != ts_col:
            mean_df = mean_df.rename(columns={mean_df.columns[0]: ts_col})
    else:
        mean_df = pd.DataFrame(columns=[ts_col])

    # combine with firsts DataFrame for non-numeric columns
    if firsts:
        firsts_df = pd.DataFrame.from_dict(firsts, orient='index')
        firsts_df = firsts_df.reset_index().rename(columns={'index': ts_col})
        merged_res = pd.merge(mean_df, firsts_df, on=ts_col, how='outer')
    else:
        merged_res = mean_df.copy()

    # sort by timestamp and finalize columns: timestamp first, then original header order except timestamp
    merged_res[ts_col] = pd.to_datetime(merged_res[ts_col], errors='coerce')
    merged_res = merged_res.sort_values(ts_col).reset_index(drop=True)
    for c in header:
        if c not in merged_res.columns:
            merged_res[c] = ''
    merged_res = merged_res[[ts_col] + [c for c in header[1:]]]

    # Format numeric columns to 3dp with comma decimal
    for c in merged_res.columns[1:]:
        coerced = pd.to_numeric(merged_res[c], errors='coerce')
        if coerced.notna().sum() > 0:
            merged_res[c] = coerced.apply(lambda v: f"{v:.3f}".replace('.', ',') if pd.notna(v) else '')
        else:
            merged_res[c] = merged_res[c].astype(str).fillna('')

    out_path = OUT_DIR / 'merged_resampled.csv'
    merged_res.to_csv(out_path, sep=';', index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)

    # Build resample summary statistics
    periods_produced = merged_res.shape[0]
    numeric_var_count = len(numeric_cols)
    non_numeric_var_count = max(0, len(header) - 1 - numeric_var_count)
    # Avoid division by zero
    reduction_factor = (rows_with_valid_ts / periods_produced) if periods_produced else float('inf')
    # numeric datapoints before/after estimation
    numeric_datapoints_before = rows_with_valid_ts * numeric_var_count
    numeric_datapoints_after = periods_produced * numeric_var_count
    numeric_reduction_pct = (1.0 - (numeric_datapoints_after / numeric_datapoints_before)) * 100.0 if numeric_datapoints_before else 0.0

    now = datetime.datetime.utcnow().isoformat() + "Z"
    summary_lines = [
        f"Resample operation summary",
        f"Timestamp (UTC): {now}",
        f"Source file: {merged_path.name}",
        f"Resample rule: {resample_rule}",
        f"Chunksize used: {chunksize}",
        f"Total rows read from source: {rows_read_total}",
        f"Rows with parsable timestamps (kept): {rows_with_valid_ts}",
        f"Output periods produced (rows in resampled): {periods_produced}",
        f"Numeric variables detected: {numeric_var_count}",
        f"Non-numeric variables preserved (first-value): {non_numeric_var_count}",
        f"Estimated numeric datapoints before resample: {numeric_datapoints_before}",
        f"Estimated numeric datapoints after resample: {numeric_datapoints_after}",
        f"Numeric datapoints reduction: {numeric_reduction_pct:.2f}%",
        f"Reduction factor (rows with timestamps / output periods): {reduction_factor:.2f}x",
        f"Note: non-numeric columns keep the first non-empty value observed for each period.",
    ]
    summary_text = "\n".join(summary_lines)
    resample_summary_path = OUT_DIR / 'resample_summary.txt'
    with resample_summary_path.open('w', encoding='utf-8') as sf:
        sf.write(summary_text + "\n")

    # cleanup temporary uploaded file if used
    if temp_path and temp_path.exists():
        try:
            temp_path.unlink()
        except Exception:
            pass

    flash(f"Resample complete: wrote {out_path.name}. Periods: {periods_produced}, numeric vars: {numeric_var_count}. Summary saved to {resample_summary_path.name}")
    return redirect(url_for('index'))

@app.route('/download/<path:filename>')
def download(filename):
    p = OUT_DIR / filename
    if not p.exists():
        flash(f"{filename} not found.")
        return redirect(url_for('index'))
    return send_from_directory(directory=str(OUT_DIR), path=filename, as_attachment=True)

if __name__ == '__main__':
    app.run(debug=True)