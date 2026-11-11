"""Replace the three panel titles of fig_rq1_heatmaps_annotated.pdf with titles that state the unit,
consistent with the RQ3 figure: (a) Throughput [x], (b) End-to-end latency [x], (c) CPU utilization [pp].
The PDF stores text as outlines, so the old titles are covered with a white box and the new titles are drawn
in Times-Bold at the same size and centre. Everything else in the figure stays untouched (vector merge).
Usage: python3 relabel_rq1_titles.py fig_rq1_heatmaps_annotated.pdf fig_rq1_heatmaps_annotated_units.pdf"""
import sys, io
from pypdf import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
SRC, OUT = sys.argv[1], sys.argv[2]
W, H = 376.0327, 168.6074
# old title bounding boxes measured at 600 dpi [pt, x from left, y from top]
OLD = [(98.4, 135.24), (194.28, 247.8), (302.4, 346.8)]
TOP, BASE, BOT = 149.4, 153.36, 154.6            # cap top, baseline, descender bottom (from top)
NEW = ['(a) Throughput [×]', '(b) End-to-end latency [×]', '(c) CPU utilization [pp]']
FS = 5.6                                          # cap height 3.7 pt / 0.662 (Times-Bold)
buf = io.BytesIO(); c = canvas.Canvas(buf, pagesize=(W, H))
for (x0, x1), t in zip(OLD, NEW):
    c.setFillColorRGB(1, 1, 1); c.rect(x0 - 0.4, H - BOT - 0.2, x1 - x0 + 0.8, BOT - TOP + 0.4, stroke=0, fill=1)
    c.setFillColorRGB(0, 0, 0); c.setFont('Times-Bold', FS); c.drawCentredString((x0 + x1) / 2, H - BASE, t)
c.save()
page = PdfReader(SRC).pages[0]; page.merge_page(PdfReader(io.BytesIO(buf.getvalue())).pages[0])
w = PdfWriter(); w.add_page(page); w.write(OUT)
