"""One-shot fixture generator. Run: uv run --with reportlab python <this file>."""
# reportlab is an ephemeral dep (uv run --with); suppress missing-import for the whole file.
# pyright: reportMissingImports=false

from pathlib import Path

from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas

out = Path(__file__).parent / "fixtures" / "simple_statement.pdf"
out.parent.mkdir(exist_ok=True)
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
