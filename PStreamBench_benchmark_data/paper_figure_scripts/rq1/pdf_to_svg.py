"""Minimal vector PDF -> SVG converter for one-page matplotlib/cairo figures.
Written because pdftocairo runs out of memory on fig_rq1_heatmaps_annotated*.pdf (its hatch pattern has a
7200 x 7200 tile that cairo rasterizes). Supports paths, fills, strokes, dashes, clipping, form XObjects,
tiling patterns (as SVG <pattern>), images (embedded PNG) and simple text drawn with standard Type1 fonts.
Usage: python3 pdf_to_svg.py in.pdf out.svg"""
import sys, io, base64
import numpy as np
from pypdf import PdfReader
from pypdf.generic import ContentStream


SRC, OUT = sys.argv[1], sys.argv[2]
R = PdfReader(SRC); P = R.pages[0]
W, H = float(P.mediabox.width), float(P.mediabox.height)
out = []; defs = []; uid = [0]
def nid(p): uid[0] += 1; return f'{p}{uid[0]}'
def M(a): return np.array([[a[0], a[1], 0], [a[2], a[3], 0], [a[4], a[5], 1]], float)
def mstr(m): return 'matrix(%.6g %.6g %.6g %.6g %.6g %.6g)' % (m[0,0], m[0,1], m[1,0], m[1,1], m[2,0], m[2,1])
def col(v): return '#%02x%02x%02x' % tuple(int(round(max(0, min(1, float(x))) * 255)) for x in v)
pat_cache = {}

def render(stream, res, base, sink):
    """stream: content object, res: resources dict, base: CTM of this stream's default space"""
    st = dict(ctm=base.copy(), fill='#000000', stroke='#000000', lw=1.0, dash=None, cap=0, join=0, ml=10, fpat=None, depth=0)
    stack = []; path = []; clip_pending = None; text = None
    ops = ContentStream(stream, R).operations
    def emit_path(fill, stroke, evenodd=False):
        if not path: return
        d = ' '.join(path)
        a = []
        if fill:
            a.append('fill="url(#%s)"' % fill_pattern() if st['fpat'] else 'fill="%s"' % st['fill'])
            if evenodd: a.append('fill-rule="evenodd"')
        else: a.append('fill="none"')
        if stroke:
            a.append('stroke="%s" stroke-width="%.4g"' % (st['stroke'], st['lw']))
            a.append('stroke-linecap="%s" stroke-linejoin="%s" stroke-miterlimit="%g"' % (['butt','round','square'][st['cap']], ['miter','round','bevel'][st['join']], st['ml']))
            if st['dash'] and st['dash'][0]: a.append('stroke-dasharray="%s" stroke-dashoffset="%g"' % (' '.join('%g' % x for x in st['dash'][0]), st['dash'][1]))
        sink.append('<path d="%s" %s/>' % (d, ' '.join(a)))
    def fill_pattern():
        name, pres = st['fpat']
        po = pres['/Pattern'][name].get_object()
        key = (id(po), tuple(np.round((M(po.get('/Matrix', [1,0,0,1,0,0])) @ base @ np.linalg.inv(st['ctm'])).ravel(), 6)))
        if key in pat_cache: return pat_cache[key]
        pid = nid('pat'); pat_cache[key] = pid
        pm = M(po.get('/Matrix', [1,0,0,1,0,0])) @ base @ np.linalg.inv(st['ctm'])
        bb = [float(x) for x in po['/BBox']]
        body = []
        render(po, po.get('/Resources', {}), np.eye(3), body)
        defs.append('<pattern id="%s" patternUnits="userSpaceOnUse" x="%g" y="%g" width="%g" height="%g" patternTransform="%s">%s</pattern>'
                    % (pid, bb[0], bb[1], float(po['/XStep']), float(po['/YStep']), mstr(pm), ''.join(body)))
        return pid
    for args, op in ops:
        op = op.decode() if isinstance(op, bytes) else op
        f = [float(x) for x in args] if op in ('m','l','c','v','y','re','cm','w','rg','RG','g','G','M') else None
        if op == 'q': stack.append(dict(st)); sink.append('<g>'); st['depth'] = 0
        elif op == 'Q':
            sink.append('</g>' * st['depth'] + '</g>'); st = stack.pop()
        elif op == 'cm':
            m = M(f); st['ctm'] = m @ st['ctm']; sink.append('<g transform="%s">' % mstr(m)); st['depth'] += 1
        elif op == 'm': path.append('M%g %g' % tuple(f))
        elif op == 'l': path.append('L%g %g' % tuple(f))
        elif op == 'c': path.append('C%g %g %g %g %g %g' % tuple(f))
        elif op == 'v': path.append('Q%g %g %g %g' % tuple(f))  # approximation, not used by cairo
        elif op == 'y': path.append('C%g %g %g %g %g %g' % (f[0], f[1], f[2], f[3], f[2], f[3]))
        elif op == 'h': path.append('Z')
        elif op == 're': x, y, w, h = f; path.append('M%g %g h%g v%g h%g Z' % (x, y, w, h, -w))
        elif op in ('W', 'W*'): clip_pending = (' '.join(path), op == 'W*')
        elif op in ('f', 'F', 'f*', 'S', 's', 'B', 'B*', 'b', 'b*', 'n'):
            if op in ('s', 'b', 'b*'): path.append('Z')
            fill = op in ('f', 'F', 'f*', 'B', 'B*', 'b', 'b*'); stroke = op in ('S', 's', 'B', 'B*', 'b', 'b*')
            if fill or stroke: emit_path(fill, stroke, op.endswith('*'))
            if clip_pending is not None:
                cid = nid('clip')
                defs.append('<clipPath id="%s"><path d="%s"%s/></clipPath>' % (cid, clip_pending[0], ' clip-rule="evenodd"' if clip_pending[1] else ''))
                # clip path coordinates are in the current user space -> wrap with identity transform group
                sink.append('<g clip-path="url(#%s)">' % cid); st['depth'] += 1; clip_pending = None
            path = []
        elif op == 'w': st['lw'] = f[0]
        elif op == 'J': st['cap'] = int(args[0])
        elif op == 'j': st['join'] = int(args[0])
        elif op == 'M': st['ml'] = f[0]
        elif op == 'd': st['dash'] = ([float(x) for x in args[0]], float(args[1]))
        elif op == 'rg': st['fill'] = col(f); st['fpat'] = None
        elif op == 'RG': st['stroke'] = col(f)
        elif op == 'g': st['fill'] = col([f[0]] * 3); st['fpat'] = None
        elif op == 'G': st['stroke'] = col([f[0]] * 3)
        elif op == 'cs': st['fcs'] = args[0]
        elif op in ('scn', 'sc'):
            if args and isinstance(args[-1], str) and args[-1].startswith('/'): st['fpat'] = (args[-1], res)
            else: st['fill'] = col([float(x) for x in args]); st['fpat'] = None
        elif op == 'gs': pass  # only /CA=/ca=1 and /SMask /None in these figures
        elif op == 'Do':
            xo = res['/XObject'][args[0]].get_object()
            if xo['/Subtype'] == '/Form':
                fm = M(xo.get('/Matrix', [1, 0, 0, 1, 0, 0]))
                sink.append('<g transform="%s">' % mstr(fm))
                bb = [float(x) for x in xo['/BBox']]
                cid = nid('clip'); defs.append('<clipPath id="%s"><rect x="%g" y="%g" width="%g" height="%g"/></clipPath>' % (cid, bb[0], bb[1], bb[2]-bb[0], bb[3]-bb[1]))
                sink.append('<g clip-path="url(#%s)">' % cid)
                render(xo, xo.get('/Resources', res), fm @ st['ctm'], sink)
                sink.append('</g></g>')
            else:
                img = xo.decode_as_image()
                bio = io.BytesIO(); img.save(bio, 'PNG')
                sink.append('<image width="1" height="1" preserveAspectRatio="none" transform="matrix(1 0 0 -1 0 1)" href="data:image/png;base64,%s"/>' % base64.b64encode(bio.getvalue()).decode())
        elif op == 'BT':   # font, size and leading are graphics state and persist across BT
            text = dict(tm=np.eye(3), size=st.get('tsize', 10), font=st.get('tfont', 'Times'), lead=st.get('tlead', 0))
        elif op == 'Tf':
            st['tsize'] = text['size'] = float(args[1])
            st['tfont'] = text['font'] = str(res['/Font'][args[0]].get_object().get('/BaseFont', '/Times-Roman'))
        elif op == 'TL': st['tlead'] = text['lead'] = float(args[0])
        elif op == 'Tm': text['tm'] = M([float(x) for x in args])
        elif op == 'Td': text['tm'] = M([1, 0, 0, 1, float(args[0]), float(args[1])]) @ text['tm']
        elif op == 'T*': text['tm'] = M([1, 0, 0, 1, 0, -text['lead']]) @ text['tm']
        elif op == 'Tj':
            s = args[0]; s = s.original_bytes.decode('cp1252') if hasattr(s, 'original_bytes') else str(s)
            bold = 'Bold' in text['font']; ital = 'Italic' in text['font']
            s = s.replace('&', '&amp;').replace('<', '&lt;')
            sink.append('<text transform="%s scale(1 -1)" font-family="Times New Roman, Times, serif" font-size="%g"%s%s fill="%s">%s</text>'
                        % (mstr(text['tm']), text['size'], ' font-weight="bold"' if bold else '', ' font-style="italic"' if ital else '', st['fill'], s))
        elif op == 'ET': text = None
    sink.append('</g>' * st['depth'])

body = []
render(P.get_contents(), P['/Resources'], np.eye(3), body)
svg = ('<?xml version="1.0" encoding="UTF-8"?>\n<svg xmlns="http://www.w3.org/2000/svg" width="%gpt" height="%gpt" viewBox="0 0 %g %g">'
       '<defs>%s</defs><g transform="matrix(1 0 0 -1 0 %g)">%s</g></svg>' % (W, H, W, H, ''.join(defs), H, ''.join(body)))
open(OUT, 'w').write(svg)
print(OUT, len(svg) // 1024, 'KiB')
