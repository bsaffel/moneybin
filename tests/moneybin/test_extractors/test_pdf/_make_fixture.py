"""One-shot fixture generator. Run: uv run --with reportlab python <this file>."""
# reportlab is an ephemeral dep (uv run --with); suppress missing-import for the whole file.
# pyright: reportMissingImports=false

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

fixtures = Path(__file__).parent / "fixtures"
fixtures.mkdir(exist_ok=True)

# ── simple_statement.pdf ──────────────────────────────────────────────────────
out = fixtures / "simple_statement.pdf"
c = canvas.Canvas(str(out), pagesize=letter)
c.setFont("Courier", 10)

# Header text (not part of the table)
c.drawString(72, 720, "ACME BANK              Account Number: ****1234")
c.drawString(72, 704, "Statement Period: 2024-01-01 to 2024-01-31")

# Table: draw ruling lines so pdfplumber's extract_tables() finds them.
# Columns: Date(72-168), Description(168-336), Amount(336-432)
# Rows: y positions for top/bottom of each row (PDF y=0 is bottom of page)
col_xs = [72, 168, 336, 432]
# Row tops in PDF coords (y increases upward); header at 680, 3 data rows below
row_ys = [680, 664, 648, 632, 616]  # 5 y-values = 4 row bands

# Draw horizontal lines
c.setLineWidth(0.5)
for y in row_ys:
    c.line(col_xs[0], y, col_xs[-1], y)

# Draw vertical lines
for x in col_xs:
    c.line(x, row_ys[0], x, row_ys[-1])

# Fill text inside cells (baseline slightly above row bottom)
rows = [
    ("Date", "Description", "Amount"),
    ("2024-01-02", "COFFEE SHOP", "-4.50"),
    ("2024-01-05", "PAYROLL DEPOSIT", "2000.00"),
    ("2024-01-09", "GROCERY MART", "-73.21"),
]
for i, (date, desc, amount) in enumerate(rows):
    text_y = row_ys[i] - 12  # 12pt below top of row band
    c.drawString(col_xs[0] + 2, text_y, date)
    c.drawString(col_xs[1] + 2, text_y, desc)
    c.drawString(col_xs[2] + 2, text_y, amount)

c.save()
print(f"wrote {out}")  # noqa: T201  # one-shot generator script

# ── empty_statement.pdf ───────────────────────────────────────────────────────
# Header text only — no table structure that pdfplumber's extract_tables() would
# detect. Used by test_import_pdf_zero_rows_raises to exercise the zero-row path.
out_empty = fixtures / "empty_statement.pdf"
c2 = canvas.Canvas(str(out_empty), pagesize=letter)
c2.setFont("Courier", 10)
c2.drawString(72, 720, "ACME BANK              Account Number: ****1234")
c2.drawString(72, 704, "Statement Period: 2024-01-01 to 2024-01-31")
c2.drawString(72, 688, "No transactions this period.")
c2.save()
print(f"wrote {out_empty}")  # noqa: T201  # one-shot generator script

# ── chase_checking_simple.pdf ─────────────────────────────────────────────────
# Single-amount column, negative_is_expense sign convention.
# Dates in MM/DD/YYYY so DEFAULT_ANCHORS period_start/end patterns match.
# Balances: opening=1000.00, closing=1126.29; row sum=+126.29 ✓
out_chase = fixtures / "chase_checking_simple.pdf"
c3 = canvas.Canvas(str(out_chase), pagesize=letter)
c3.setFont("Courier", 10)

# Header lines — text patterns chosen to match DEFAULT_ANCHORS exactly:
#   account_id:       Account\s+Number[:\s]+(\S+)  → captures "1234"
#   period_start:     Statement\s+Period:\s+(\d{2}/\d{2}/\d{4}) → captures "01/01/2024"
#   period_end:       (?:through|to|–|-)\s+(\d{2}/\d{2}/\d{4})\s*$ → captures "01/31/2024"
#   opening_balance:  Beginning\s+Balance[:\s]+\$?([\d,]+\.\d{2}) → captures "1000.00"
#   closing_balance:  Ending\s+Balance[:\s]+\$?([\d,]+\.\d{2}) → captures "1126.29"
c3.drawString(72, 740, "Chase Bank")
c3.drawString(72, 724, "Account Number: 1234")
c3.drawString(72, 708, "Statement Period: 01/01/2024 to 01/31/2024")
c3.drawString(72, 692, "Beginning Balance: $1000.00")
c3.drawString(72, 676, "Ending Balance: $1126.29")

# Table: Date | Description | Amount
# Columns wide enough for 10-char dates (MM/DD/YYYY), descriptions, amounts.
chase_col_xs = [72, 168, 360, 456]
# 6 row bands: header + 5 data rows
chase_row_ys = [660, 644, 628, 612, 596, 580, 564]

c3.setLineWidth(0.5)
for y in chase_row_ys:
    c3.line(chase_col_xs[0], y, chase_col_xs[-1], y)
for x in chase_col_xs:
    c3.line(x, chase_row_ys[0], x, chase_row_ys[-1])

# Row sums: -4.50 + 2000.00 + -73.21 + -150.00 + -1646.00 = 126.29
# closing - opening = 1126.29 - 1000.00 = 126.29 ✓
chase_rows = [
    ("Date", "Description", "Amount"),
    ("01/02/2024", "COFFEE SHOP", "-4.50"),
    ("01/05/2024", "PAYROLL DEPOSIT", "2000.00"),
    ("01/09/2024", "GROCERY MART", "-73.21"),
    ("01/15/2024", "UTILITIES PAYMENT", "-150.00"),
    ("01/22/2024", "REFUND", "-1646.00"),
]
for i, (date, desc, amount) in enumerate(chase_rows):
    text_y = chase_row_ys[i] - 12
    c3.drawString(chase_col_xs[0] + 2, text_y, date)
    c3.drawString(chase_col_xs[1] + 2, text_y, desc)
    c3.drawString(chase_col_xs[2] + 2, text_y, amount)

c3.save()
print(f"wrote {out_chase}")  # noqa: T201  # one-shot generator script

# ── chase_checking_unruled.pdf ────────────────────────────────────────────────
# The SAME statement as chase_checking_simple.pdf, but with NO ruling lines —
# columns are whitespace-aligned, which is how real bank statements are actually
# typeset. pdfplumber's extract_tables() finds nothing here (it keys on ruling
# lines), so doc.tables is empty and only doc.text_lines carries the rows.
#
# This fixture exists because every other fixture in this file deliberately
# draws ruling lines "so pdfplumber's extract_tables() finds them" — which made
# the suite structurally incapable of catching F10, where a real Chase statement
# extracted 0 transactions. Recipe derivation must work from text_lines alone.
out_unruled = fixtures / "chase_checking_unruled.pdf"
c6 = canvas.Canvas(str(out_unruled), pagesize=letter)
c6.setFont("Courier", 10)

c6.drawString(72, 740, "Chase Bank")
c6.drawString(72, 724, "Account Number: 1234")
c6.drawString(72, 708, "Statement Period: 01/01/2024 to 01/31/2024")
c6.drawString(72, 692, "Beginning Balance: $1000.00")
c6.drawString(72, 676, "Ending Balance: $1126.29")
c6.drawString(72, 652, "ACCOUNT ACTIVITY")

# Same rows and same reconciliation as chase_checking_simple.pdf:
# -4.50 + 2000.00 + -73.21 + -150.00 + -1646.00 = 126.29 = 1126.29 - 1000.00 ✓
unruled_rows = [
    ("Date", "Description", "Amount"),
    ("01/02/2024", "COFFEE SHOP", "-4.50"),
    ("01/05/2024", "PAYROLL DEPOSIT", "2000.00"),
    ("01/09/2024", "GROCERY MART", "-73.21"),
    ("01/15/2024", "UTILITIES PAYMENT", "-150.00"),
    ("01/22/2024", "REFUND", "-1646.00"),
]
unruled_y = 632
for date, desc, amount in unruled_rows:
    c6.drawString(72, unruled_y, date)
    c6.drawString(168, unruled_y, desc)
    c6.drawString(360, unruled_y, amount)
    unruled_y -= 16

c6.save()
print(f"wrote {out_unruled}")  # noqa: T201  # one-shot generator script

# ── amex_credit_simple.pdf ────────────────────────────────────────────────────
# Credit card statement, negative_is_expense convention (charges are negative
# row amounts). Balance anchors capture unsigned digits — r"([\d,]+\.\d{2})"
# strips any leading minus — so we model opening=500.00, closing=252.50 so the
# expected_delta = closing - opening = -247.50 matches the row sum of -247.50
# (charges of 89.99 + 15.99 + 141.52, each stored as negative). Both balances
# are positive on the page; the convention flows through the row amounts.
out_amex = fixtures / "amex_credit_simple.pdf"
c4 = canvas.Canvas(str(out_amex), pagesize=letter)
c4.setFont("Courier", 10)

c4.drawString(72, 740, "American Express")
c4.drawString(72, 724, "Account Number: 5678")
c4.drawString(72, 708, "Statement Period: 02/01/2024 to 02/29/2024")
# opening=500.00, closing=252.50; closing-opening=-247.50; row sum=-247.50 ✓
c4.drawString(72, 692, "Beginning Balance: $500.00")
c4.drawString(72, 676, "Ending Balance: $252.50")

amex_col_xs = [72, 168, 360, 456]
amex_row_ys = [660, 644, 628, 612, 596]  # header + 3 data rows

c4.setLineWidth(0.5)
for y in amex_row_ys:
    c4.line(amex_col_xs[0], y, amex_col_xs[-1], y)
for x in amex_col_xs:
    c4.line(x, amex_row_ys[0], x, amex_row_ys[-1])

# -89.99 + -15.99 + -141.52 = -247.50; closing - opening = 252.50 - 500.00 = -247.50 ✓
amex_rows = [
    ("Date", "Description", "Amount"),
    ("02/03/2024", "AMAZON.COM", "-89.99"),
    ("02/10/2024", "NETFLIX SUBSCRIPT", "-15.99"),
    ("02/18/2024", "WHOLE FOODS", "-141.52"),
]
for i, (date, desc, amount) in enumerate(amex_rows):
    text_y = amex_row_ys[i] - 12
    c4.drawString(amex_col_xs[0] + 2, text_y, date)
    c4.drawString(amex_col_xs[1] + 2, text_y, desc)
    c4.drawString(amex_col_xs[2] + 2, text_y, amount)

c4.save()
print(f"wrote {out_amex}")  # noqa: T201  # one-shot generator script

# ── fidelity_positions.pdf ────────────────────────────────────────────────────
# Investment positions table — NOT a transaction shape (no Date column, no Amount
# column). Routing returns "no_transaction_table" → seed path.
out_fidelity = fixtures / "fidelity_positions.pdf"
c5 = canvas.Canvas(str(out_fidelity), pagesize=letter)
c5.setFont("Courier", 10)

c5.drawString(72, 740, "Fidelity Investments")
c5.drawString(72, 724, "Account Number: 9012")

# Table: Symbol | Shares | Price | Value — no Date column, no Amount column.
# _DATE_COL_RE requires first header to match ^(date|trans.*date|posting.*date)$;
# "Symbol" does not match, so _is_transaction_shaped returns False.
fid_col_xs = [72, 168, 264, 360, 456]
fid_row_ys = [700, 684, 668, 652, 636]  # header + 3 data rows

c5.setLineWidth(0.5)
for y in fid_row_ys:
    c5.line(fid_col_xs[0], y, fid_col_xs[-1], y)
for x in fid_col_xs:
    c5.line(x, fid_row_ys[0], x, fid_row_ys[-1])

fid_rows = [
    ("Symbol", "Shares", "Price", "Value"),
    ("AAPL", "100", "180.00", "18000.00"),
    ("MSFT", "50", "350.00", "17500.00"),
    ("VTI", "25", "220.00", "5500.00"),
]
for i, row_data in enumerate(fid_rows):
    text_y = fid_row_ys[i] - 12
    for j, cell in enumerate(row_data):
        c5.drawString(fid_col_xs[j] + 2, text_y, cell)

c5.save()
print(f"wrote {out_fidelity}")  # noqa: T201  # one-shot generator script
