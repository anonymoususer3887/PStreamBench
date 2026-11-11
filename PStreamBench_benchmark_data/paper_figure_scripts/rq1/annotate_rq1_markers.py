"""Overlay numbered boxes on the RQ1 heatmap PDF (vector, original untouched)."""
import sys
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import Color, black, white, HexColor
MK = {'1':'#1565C0','2':'#EF6C00','3':'#C62828','4':'#00897B','5':'#F9A825','6':'#212121'}  # one colour per marker, distinct from the green/magenta scale
SRC, OUT = sys.argv[1], sys.argv[2]
H = 255.6; S = 0.25                      # page height [pt], px(288 dpi) -> pt
Y0, RH = 101, 53.357                     # first row top [px], row height [px]
PANEL = {'a': 411, 'b': 968, 'c': 1521}; CW = 105.6   # panel left edge [px], column width [px]
COLS = ['flink', 'storm', 'spark', 'kafka', 'risingwave']
ROWS = ['SA','TM','TT','MO','AD','CA','LR','LA','BI','SG','GCM','SD','TPCH','WC']
def rect(panel, c0, c1, r0, r1):
    x0 = (PANEL[panel] + COLS.index(c0) * CW) * S; x1 = (PANEL[panel] + (COLS.index(c1) + 1) * CW) * S
    y0 = H - (Y0 + (ROWS.index(r1) + 1) * RH) * S; y1 = H - (Y0 + ROWS.index(r0) * RH) * S
    return x0, y0, x1, y1
# (label, panel, first col, last col, first row, last row, label position)
BOXES = [
  ('1', 'a', 'flink', 'risingwave', 'SA', 'MO', 'tl'),        # Finding 1: CPU-intensive rows scale
  ('2', 'a', 'flink', 'risingwave', 'LA', 'SD', 'tl'),        # Finding 1: State+UDF rows stay flat
  ('3', 'b', 'flink', 'flink', 'AD', 'CA', 'tl'),             # Finding 2: Flink pays in latency
  ('3', 'b', 'flink', 'flink', 'LA', 'LA', None),
  ('4', 'c', 'storm', 'storm', 'SA', 'WC', 'tl'),             # Finding 2: Storm pays in CPU
  ('5', 'a', 'kafka', 'kafka', 'MO', 'AD', 'tr'),             # Finding 2: Kafka Streams pays in throughput
  ('5', 'c', 'kafka', 'kafka', 'MO', 'AD', 'tr'),
  ('6', 'a', 'spark', 'spark', 'TM', 'TM', 'tr'),             # Finding 2: Spark trades batch size for latency
  ('6', 'b', 'spark', 'spark', 'TM', 'TM', 'tr'),
  ('6', 'b', 'spark', 'spark', 'AD', 'AD', None),
]
c = canvas.Canvas('overlay.pdf', pagesize=(515.52, H))
for lab, p, c0, c1, r0, r1, pos in BOXES:
    x0, y0, x1, y1 = rect(p, c0, c1, r0, r1)
    HL = HexColor(MK[lab])
    c.setStrokeColor(white); c.setLineWidth(2.8); c.setDash()   # white halo keeps the box visible on dark cells
    c.rect(x0 + 0.6, y0 + 0.6, x1 - x0 - 1.2, y1 - y0 - 1.2, stroke=1, fill=0)
    c.setStrokeColor(HL); c.setLineWidth(1.5)
    c.rect(x0 + 0.6, y0 + 0.6, x1 - x0 - 1.2, y1 - y0 - 1.2, stroke=1, fill=0)
    if pos:
        cx = x0 + 0.2 if pos == 'tl' else x1 - 0.2; cy = y1 - 0.2
        c.setFillColor(HL); c.setStrokeColor(HL); c.setLineWidth(0.7)
        c.circle(cx, cy, 3.4, stroke=1, fill=1)
        c.setFillColor(black if lab=='5' else white); c.setFont('Helvetica-Bold', 5.2)
        c.drawCentredString(cx, cy - 1.95, lab)
c.save()
base = PdfReader(SRC); ov = PdfReader('overlay.pdf')
page = base.pages[0]; page.merge_page(ov.pages[0])
w = PdfWriter(); w.add_page(page); w.write(OUT)
