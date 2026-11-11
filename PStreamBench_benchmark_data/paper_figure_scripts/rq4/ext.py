"""Extract the plotted values of fig_rq4_summary_1M_v1.svg (dots, boxes, bars, diamonds) into extract.json.
Usage: python3 ext.py ../inputs/fig_rq4_summary_1M_v1.svg"""
import xml.etree.ElementTree as ET,re,numpy as np,json,sys,os
HERE=os.path.dirname(os.path.abspath(__file__)); SRC=sys.argv[1] if len(sys.argv)>1 else os.path.join(HERE,'..','inputs','fig_rq4_summary_1M_v1.svg')
ns={'s':'http://www.w3.org/2000/svg'}
p=ET.XMLParser(target=ET.TreeBuilder(insert_comments=True))
r=ET.parse(SRC,parser=p).getroot()
fig=r.find('.//s:g[@id="figure_1"]',ns)
axes=[a for a in fig.findall('s:g',ns) if a.get('id','').startswith('axes')]
def val(s):
    s=s.strip().replace('×','')
    m={'k':1e3,'M':1e6}
    return float(s[:-1])*m[s[-1]] if s[-1] in m else float(s)
def pts(d): return [tuple(map(float,xy)) for xy in re.findall(r'([-\d.]+) ([-\d.]+)',d)]
OUT={}
for ai,ax in enumerate(axes):
    ya=[g for g in ax.findall('s:g',ns) if g.get('id','').startswith('matplotlib.axis')][1]
    T=[]
    for tk in ya.findall('s:g',ns):
        c=[n.text for n in tk.iter() if n.tag is ET.Comment]
        us=[u for u in tk.findall('.//s:use',ns) if u.get('y') is not None]
        if c and us: T.append((float(us[0].get('y')),np.log10(val(c[0]))))
    T=np.array(T); b,a=np.polyfit(T[:,1],T[:,0],1)   # y = a + b*log10(v)
    tov=lambda y:10**((y-a)/b)
    dots=[]
    for pc in ax.findall('s:g',ns):
        if pc.get('id','').startswith('PathCollection'):
            for u in pc.findall('.//s:use',ns):
                col=re.search(r'fill: (#[0-9a-f]+)',u.get('style','')); st=u.get('style','')
                ec=re.search(r'stroke: (#[0-9a-f]+)',st)
                dots.append(dict(pc=pc.get('id'),x=float(u.get('x')),v=tov(float(u.get('y'))),fill=col.group(1) if col else None,stroke=ec.group(1) if ec else None))
    patches=[];lines=[]
    for g in ax.findall('s:g',ns):
        gid=g.get('id','')
        if gid.startswith('patch') or gid.startswith('line2d'):
            for pth in g.findall('s:path',ns):
                P=pts(pth.get('d',''));st=pth.get('style','')
                if not P: continue
                fill=re.search(r'fill: (#[0-9a-f]+)',st);stroke=re.search(r'stroke: (#[0-9a-f]+)',st);op=re.search(r'fill-opacity: ([\d.]+)',st);lw=re.search(r'stroke-width: ([\d.]+)',st)
                rec=dict(id=gid,x=[q[0] for q in P],v=[tov(q[1]) for q in P],fill=fill.group(1) if fill else None,stroke=stroke.group(1) if stroke else None,op=op.group(1) if op else None,lw=lw.group(1) if lw else None)
                (patches if gid.startswith('patch') else lines).append(rec)
    OUT[ai]=dict(dots=dots,patches=patches,lines=lines)
json.dump(OUT,open(os.path.join(HERE,'extract.json'),'w'))
for ai in OUT:
    print('axes',ai+1,'dots',len(OUT[ai]['dots']),'patches',len(OUT[ai]['patches']),'lines',len(OUT[ai]['lines']))
    fl=(set((p['fill'],p['op']) for p in OUT[ai]['patches'])); print(' patch fills',fl)
    sl=(set((l['stroke'],l['lw']) for l in OUT[ai]['lines'])); print(' line strokes',sl)
