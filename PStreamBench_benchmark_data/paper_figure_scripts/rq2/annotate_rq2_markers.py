"""Overlay numbered boxes on the RQ2 heatmap PDF (vector; original figure untouched)."""
import sys
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, white, HexColor
SRC, OUT = sys.argv[1], sys.argv[2]
PW, H = 515.52, 259.2; S = 0.25                      # page size [pt]; px at 288 dpi -> pt
Y0, RH = 124, 53.0                                   # first row top [px], row height [px]
PANEL = {'a': 401, 'b': 820, 'c': 1239, 'd': 1659}; CW = 65.5
COLS = ['f-m510', 'f-25g', 'f-100g', 's-m510', 's-25g', 's-100g']
ROWS = ['SA','TM','TT','MO','AD','CA','LR','LA','BI','SG','GCM','SD','TPCH','WC']
MK = {'1':'#1565C0','2':'#EF6C00','3':'#C62828','4':'#00897B','5':'#F9A825','6':'#212121'}
def rect(panel, c0, c1, r0, r1):
    x0 = (PANEL[panel] + COLS.index(c0) * CW) * S; x1 = (PANEL[panel] + (COLS.index(c1) + 1) * CW) * S
    y0 = H - (Y0 + (ROWS.index(r1) + 1) * RH) * S; y1 = H - (Y0 + ROWS.index(r0) * RH) * S
    return x0, y0, x1, y1
BOXES = [
  ('1', 'a', 'f-m510', 'f-100g', 'SA', 'TM', 'tl'),   # F1: compute-bound workloads convert cores into throughput
  ('2', 'a', 'f-m510', 'f-100g', 'AD', 'CA', 'tl'),   # F2: Flink throughput gain shrinks on stronger hardware
  ('3', 'b', 'f-25g',  'f-100g', 'AD', 'AD', 'tl'),   # F2: Flink latency cost explodes with core-matched DoP
  ('3', 'b', 'f-m510', 'f-100g', 'WC', 'WC', 'tl'),
  ('4', 'c', 's-m510', 's-100g', 'SA', 'WC', 'tr'),   # F3: Storm pays in CPU ...
  ('4', 'd', 's-m510', 's-100g', 'SA', 'WC', 'tr'),   #     ... and memory
  ('5', 'a', 's-m510', 's-100g', 'TT', 'TT', None),   # F3: windowed Storm pipelines need parallelism to progress
  ('5', 'a', 's-m510', 's-100g', 'CA', 'CA', None),
  ('5', 'a', 's-m510', 's-100g', 'LA', 'LA', 'tr'),
  ('6', 'a', 's-m510', 's-100g', 'SA', 'TM', 'tr'),   # F3: CPU-intensive Storm pipelines lose throughput
]
c = canvas.Canvas('overlay_rq2.pdf', pagesize=(PW, H))
for lab, p, c0, c1, r0, r1, pos in BOXES:
    x0, y0, x1, y1 = rect(p, c0, c1, r0, r1); HL = HexColor(MK[lab])
    for col, lw in ((white, 2.8), (HL, 1.5)):        # white halo, then coloured outline
        c.setStrokeColor(col); c.setLineWidth(lw)
        c.rect(x0 + 0.6, y0 + 0.6, x1 - x0 - 1.2, y1 - y0 - 1.2, stroke=1, fill=0)
    if pos:
        cx = x0 + 0.2 if pos == 'tl' else x1 - 0.2; cy = y1 - 0.2
        c.setFillColor(HL); c.setStrokeColor(white); c.setLineWidth(0.6); c.circle(cx, cy, 3.4, stroke=1, fill=1)
        c.setFillColor(black if lab == '5' else white); c.setFont('Helvetica-Bold', 5.2); c.drawCentredString(cx, cy - 1.9, lab)
c.save()
page = PdfReader(SRC).pages[0]; page.merge_page(PdfReader('overlay_rq2.pdf').pages[0])
w = PdfWriter(); w.add_page(page); w.write(OUT)
