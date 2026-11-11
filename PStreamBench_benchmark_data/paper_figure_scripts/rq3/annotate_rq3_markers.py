"""Overlay numbered boxes on the RQ3 heatmap PDF (vector; original figure untouched)."""
import sys
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.colors import black, white, HexColor
SRC, OUT = sys.argv[1], sys.argv[2]
PW, H = 515.52, 259.2; S = 0.25
Y0, RH = 171, 49.71
PANEL = {'a': 401, 'b': 961, 'c': 1522}; CW = 53.0
COLS = ['f-LM','f-MH','s-LM','s-MH','sp-LM','sp-MH','k-LM','k-MH','r-LM','r-MH']
ROWS = ['SA','TM','TT','MO','AD','CA','LR','LA','BI','SG','GCM','SD','TPCH','WC']
MK = {'1':'#1565C0','2':'#EF6C00','3':'#C62828','4':'#00897B','5':'#F9A825','6':'#212121'}
def rect(panel, c0, c1, r0, r1):
    x0 = (PANEL[panel] + COLS.index(c0) * CW) * S; x1 = (PANEL[panel] + (COLS.index(c1) + 1) * CW) * S
    y0 = H - (Y0 + (ROWS.index(r1) + 1) * RH) * S; y1 = H - (Y0 + ROWS.index(r0) * RH) * S
    return x0, y0, x1, y1
BOXES = [
  # Finding 1: latency jumps in the first step and stays flat in the second
  ('1','b','f-LM','f-MH','TM','TM','tl'), ('1','b','f-LM','f-MH','AD','AD',None), ('1','b','f-LM','f-MH','CA','CA',None),
  ('1','b','f-LM','f-MH','WC','WC',None), ('1','b','s-LM','s-MH','LA','LA','tl'),
  # Finding 1: Spark keeps pace in throughput, pays in latency (first step)
  ('2','a','sp-LM','sp-LM','SA','WC','tl'), ('2','b','sp-LM','sp-LM','SA','WC','tl'),
  # Finding 1: beyond saturation, output falls
  ('3','a','s-LM','s-MH','LA','LA','tl'), ('3','a','k-LM','k-MH','TM','TM','tr'), ('3','a','f-LM','f-MH','SD','SD','tl'),
  # Finding 1: Flink TPC-H latency decreases with load (buffer timeout)
  ('4','b','f-LM','f-MH','TPCH','TPCH','tl'),
  # Finding 3: Kafka Streams CPU rises while output falls
  ('5','c','k-LM','k-MH','SA','WC','tr'),
  # Finding 3: Storm CPU barely changes
  ('6','c','s-LM','s-MH','SA','WC','tr'),
]
c = canvas.Canvas('overlay_rq3.pdf', pagesize=(PW, H))
for lab, p, c0, c1, r0, r1, pos in BOXES:
    x0, y0, x1, y1 = rect(p, c0, c1, r0, r1); HL = HexColor(MK[lab])
    for col, lw in ((white, 2.8), (HL, 1.5)):
        c.setStrokeColor(col); c.setLineWidth(lw)
        c.rect(x0 + 0.6, y0 + 0.6, x1 - x0 - 1.2, y1 - y0 - 1.2, stroke=1, fill=0)
    if pos:
        cx = x0 + 0.2 if pos == 'tl' else x1 - 0.2; cy = y1 - 0.2
        c.setFillColor(HL); c.setStrokeColor(white); c.setLineWidth(0.6); c.circle(cx, cy, 3.4, stroke=1, fill=1)
        c.setFillColor(black if lab == '5' else white); c.setFont('Helvetica-Bold', 5.2); c.drawCentredString(cx, cy - 1.9, lab)
c.save()
page = PdfReader(SRC).pages[0]; page.merge_page(PdfReader('overlay_rq3.pdf').pages[0])
w = PdfWriter(); w.add_page(page); w.write(OUT)
