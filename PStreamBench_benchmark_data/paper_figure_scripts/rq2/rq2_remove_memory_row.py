"""Remove the memory row from the RQ2 detail figure (fig_rq2_detail_40.pdf) without re-plotting.
The page is cut with clipping: rows 1-3 (throughput, latency, CPU) are kept, the memory row is removed, and the
x-tick-label strip of the original bottom row is moved directly below the CPU row. All content stays vector.
Usage: python3 rq2_remove_memory_row.py ../inputs/fig_rq2_detail_40.pdf fig_rq2_detail_40_nomem.pdf
Coordinates (pt, measured from the top of the 489.6 x 255.6 pt page): CPU axis bottom 186.8, memory axis bottom 236.4."""
import sys
from pypdf import PdfReader, PdfWriter, PageObject, Transformation
from pypdf.generic import ContentStream
SRC, OUT = sys.argv[1], sys.argv[2]
W, H = 489.6, 255.6
CUT = 236.4 - 186.8            # height removed: memory row plus gap
NH = H - CUT
def clipped(x, y, w, h):
    pg = PdfReader(SRC).pages[0]
    data = pg.get_contents().get_data()
    cs = ContentStream(None, pg.pdf)
    cs.set_data(f"q {x} {y} {w} {h} re W n\n".encode() + data + b"\nQ\n")
    pg.replace_contents(cs)
    return pg
out = PageObject.create_blank_page(width=W, height=NH)
out.merge_transformed_page(clipped(0, H - 189.5, W, 189.5), Transformation().translate(0, -CUT))  # rows 1-3, legend
out.merge_transformed_page(clipped(30, 0, W - 30, H - 237.0), Transformation())                 # x tick labels, 'Uniform DoP'
w = PdfWriter(); w.add_page(out); w.pages[0].compress_content_streams(); w.write(OUT)
