"""RQ3 heatmap DoP 2 -> 40 with the user-supplied estimated cells (rq3_estimated_cells.csv, table 'all_estimated_cells').
Usage: python3 rq3_dop_load_estimated.py [LH|LMH]
Measured values: rq3_dop_load_LMH_values.csv (written by rq3_dop_load_LMH.py; same filters, median over runs).
An estimate is used only where the measured value is missing or <= 0 (throughput, latency ratio) or missing (CPU);
a measured value is never replaced. Estimated and uniform_1-fallback cells are drawn like measured cells (column 'source' in the values CSV says which is which).\nOptional 2nd argument 'marked': numbered boxes for the insights listed in INSIGHTS (output *_marked.*).
Cells with neither a measured nor an estimated value: grey, 'n/a' (no hatching).
Output: fig_rq3_dop_load_est_{MODE}.pdf/svg/png and rq3_dop_load_est_{MODE}_values.csv (column 'source' = measured/estimated/none)."""
import os, sys, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.lines import Line2D; from matplotlib.patches import Patch
HERE=os.path.dirname(os.path.abspath(__file__)); MODE=sys.argv[1] if len(sys.argv)>1 else 'LMH'; MARK=len(sys.argv)>2 and sys.argv[2]=='marked'
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':6.5,'axes.linewidth':0.5,'pdf.fonttype':42,'svg.fonttype':'none'})
out=pd.read_csv(os.path.join(HERE,'rq3_dop_load_LMH_values.csv'))
E=pd.read_csv(os.path.join(HERE,'rq3_estimated_cells.csv'))
APPS=[('Sentiment Analysis','SA','CPU'),('Traffic Monitoring','TM','CPU'),('Trending Topics','TT','CPU'),('Machine Outlier','MO','CPU'),
      ('Ad Analytics','AD','S+N'),('Click Analytics','CA','State'),('Linear Road','LR','State'),
      ('Log Processing','LA','State+UDF'),('Bargain Index','BI','State+UDF'),('Smart Grid','SG','State+UDF'),('Google Cloud Monitoring','GCM','State+UDF'),('Spike Detection','SD','State+UDF'),
      ('TPCH','TPCH','Stateless'),('Word Count','WC','Stateless')]
AB={ab:a for a,ab,_ in APPS}; LONG={'Log Processing':'Log Analyzer','TPCH':'TPC-H','Google Cloud Monitoring':'Cloud Monitoring'}
SYS=['flink','storm','spark','kafkastream','risingwave']; SN=['Flink','Storm','Spark','Kafka S.','RisingW.']
RATES,RL=([1e5,1e6],['L','H']) if MODE=='LH' else ([1e5,5e5,1e6],['L','M','H'])
LR={'L':1e5,'M':5e5,'H':1e6}; NR=len(RATES)
EST={(AB[r.abbr],r.system,LR[r.load],r.metric):r.value for r in E.itertuples()}
cm=LinearSegmentedColormap.from_list('pg',['#8E0152','#DE77AE','#FDE0EF','#F7F7F7','#E6F5D0','#7FBC41','#276419'])
cm_cpu=LinearSegmentedColormap.from_list('cpu',['#276419','#B8E186','#F7F7F7','#F1B6DA','#C51B7D'])
rows=[]; conflicts=[]
def fmt(v,signed=False):
    # never rounds a non-zero value to 0 or to a whole number: >=10 one decimal, 1-10 two decimals,
    # <1 two significant digits (0.90, 0.074, 0.00052); exact zero stays 0.0
    a=abs(v); sg='+' if signed and v>=0 else ''
    if a==0: return sg+'0.0'
    d=1 if a>=10 else (2 if a>=1 else max(2,int(-np.floor(np.log10(a)))+1))
    if signed and a>=0.1: d=1
    return f'{v:{sg}.{d}f}'
def cell(a,s,r,col):
    x=out[(out.application==a)&(out.system==s)&(out.rate==r)]
    v=x[col].iloc[0] if not x.empty else np.nan
    fb=(not x.empty) and ((x.src_dop2.iloc[0]=='uniform_1') or (x.src_dop40.iloc[0]=='uniform_1'))
    ok=not np.isnan(v) and (col=='cpu_diff_pp' or v>0)
    e=EST.get((a,s,r,col))
    if ok:
        if e is not None: conflicts.append((a,s,r,col,v,e))
        src='measured'
    elif e is not None: v,src=e,'estimated'
    else: v,src=np.nan,'none'
    rows.append(dict(application=a,system=s,rate=r,metric=col,value=v,source=src,uniform_1_fallback=fb)); return v,src,fb
nRw=len(APPS); nC=len(SYS)*NR
fig,axs=plt.subplots(1,3,figsize=(7.16,3.4)); fig.subplots_adjust(left=0.175,right=0.995,top=0.86,bottom=0.15,wspace=0.06)
panels=[('thr_ratio','(a) Throughput [$\\times$]','green = increases',True),('lat_ratio','(b) E2E latency [$\\times$]','green = decreases',False),('cpu_diff_pp','(c) CPU utilization [pp]','%-points',None)]
fs=3.4 if NR==3 else 4.2
for k,(col,title,sub,better_high) in enumerate(panels):
    ax=axs[k]; ax.set_xlim(0,nC); ax.set_ylim(nRw,0)
    for i,(a,ab,cat) in enumerate(APPS):
        for j,s in enumerate(SYS):
            for q,r in enumerate(RATES):
                x=j*NR+q; v,src,fb=cell(a,s,r,col)
                if src=='none':
                    ax.add_patch(plt.Rectangle((x,i),1,1,facecolor='#d9d9d9',edgecolor='white',lw=0.4))
                    ax.text(x+0.5,i+0.55,'n/a',ha='center',va='center',fontsize=fs-0.6,color='0.35'); continue
                if col=='cpu_diff_pp': c=cm_cpu(np.clip((v+60)/120,0,1)); txt=fmt(v,True)
                else:
                    t=np.log10(v) if better_high else -np.log10(v); c=cm(np.clip(0.5+t/4,0,1))
                    txt=fmt(v)
                ax.add_patch(plt.Rectangle((x,i),1,1,facecolor=c,edgecolor='white',lw=0.4))
                lum=np.dot(c[:3],[0.299,0.587,0.114])
                ax.text(x+0.5,i+0.58,txt,ha='center',va='center',fontsize=fs*min(1,5.2/len(txt)),color='white' if lum<0.5 else 'black')
    for j in range(len(SYS)+1): ax.axvline(j*NR,color='0.25',lw=0.6)
    for b in [4,5,7,12]: ax.axhline(b,color='0.25',lw=0.6)
    ax.set_xticks([x+0.5 for x in range(nC)]); ax.set_xticklabels(RL*len(SYS),fontsize=4.6); ax.xaxis.tick_top(); ax.tick_params(length=0,pad=1)
    for j,n in enumerate(SN): ax.text(j*NR+NR/2,-1.05,n,ha='center',va='bottom',fontsize=6.5 if NR==3 else 5.8,fontweight='bold')
    ax.set_yticks([i+0.5 for i in range(nRw)])
    if k==0:
        ax.set_yticklabels([f'{LONG.get(a,a)} ({ab})' for a,ab,_ in APPS],fontsize=5.6)
        for cat,(y0,y1) in {'CPU':(0,4),'S+N':(4,5),'State':(5,7),'State+UDF':(7,12),'Stateless':(12,14)}.items():
            ax.text(-0.62,1-(y0+y1)/2/nRw,cat,rotation=90,ha='center',va='center',fontsize=5.6,style='italic',transform=ax.transAxes)
    else: ax.set_yticklabels([])
    for sp in ax.spines.values(): sp.set_linewidth(0.6)
    ax.text(0.5,-0.03,title,ha='center',va='top',fontsize=7,fontweight='bold',transform=ax.transAxes)
    ax.text(0.5,-0.085,sub,ha='center',va='top',fontsize=5.4,style='italic',transform=ax.transAxes)
rates='L = 100k and H = 1M' if MODE=='LH' else 'L = 100k, M = 500k and H = 1M'
fig.text(0.56,0.965,f'Change from DoP 2 to DoP 40 at {rates} events/s, m510',ha='center',fontsize=6.3)
MK={'1':'#1565C0','2':'#EF6C00','3':'#C62828','4':'#00897B','5':'#6A1B9A'}
ROWS=[ab for _,ab,_ in APPS]
# (label, panel 0/1/2, system, first row, last row, rates or None = all, label corner)
BOXES=[('1',0,'flink','TM','TT',None,'tl'),('1',0,'spark','TM','TM',None,'tl'),
       ('2',0,'storm','TT','CA',None,'tl'),('2',0,'storm','LA','LA',None,None),('2',2,'storm','SA','WC',None,'tl'),
       ('3',1,'flink','AD','CA',[5e5,1e6],'tl'),('3',1,'flink','LA','LA',[1e6],None),
       ('4',0,'kafkastream','AD','CA',[5e5,1e6],'tl'),('4',2,'kafkastream','AD','CA',[5e5,1e6],'tl'),
       ('5',0,'risingwave','SA','WC',None,'tr'),('5',2,'risingwave','SA','WC',None,'tr')]
INSIGHTS={'1':'CPU-intensive queries: the gain from DoP 40 grows from L to H',
          '2':'Storm: largest throughput gains, paid with +11.3 to +62.6 pp CPU',
          '3':'Flink stateful/join queries: throughput gain bought with latency',
          '4':'Kafka Streams: under load, throughput falls while CPU rises',
          '5':'RisingWave: 30 of 42 cells within 0.8-1.3x, CPU change < 2 pp in 40 of 42'}
_V=pd.DataFrame(rows).drop_duplicates(['application','system','rate','metric'])
_rt=_V[(_V.system=='risingwave')&(_V.metric=='thr_ratio')].value; _rc=_V[(_V.system=='risingwave')&(_V.metric=='cpu_diff_pp')].value
_sc=_V[(_V.system=='storm')&(_V.metric=='cpu_diff_pp')].value
INSIGHTS['2']=f'Storm: largest throughput gains, paid with {_sc.min():+.1f} to {_sc.max():+.1f} pp CPU'
INSIGHTS['5']=f'RisingWave: {int(((_rt>=0.8)&(_rt<=1.3)).sum())} of {len(_rt)} cells within 0.8-1.3x, CPU change < 2 pp in {int((_rc.abs()<2).sum())} of {len(_rc)}'
if MARK:
    for lab,k,sname,r0,r1,rs,pos in BOXES:
        rs=[r for r in (rs or RATES) if r in RATES]
        if not rs: continue
        x0=SYS.index(sname)*NR+RATES.index(min(rs)); x1=SYS.index(sname)*NR+RATES.index(max(rs))+1
        y0=ROWS.index(r0); y1=ROWS.index(r1)+1; ax=axs[k]; c=MK[lab]; e=0.0
        for col,lw in (('white',1.5),(c,0.9)):
            ax.add_patch(plt.Rectangle((x0+e,y0+e),x1-x0-2*e,y1-y0-2*e,fill=False,edgecolor=col,lw=lw,clip_on=False,zorder=5))
        if pos:
            cx=x0+0.1 if pos=='tl' else x1-0.1
            ax.plot(cx,y0+0.1,'o',ms=6.0,mfc=c,mec='white',mew=0.5,clip_on=False,zorder=6)
            ax.text(cx,y0+0.13,lab,ha='center',va='center',fontsize=4.8,color='white',fontweight='bold',zorder=7,clip_on=False)
SUF='_marked' if MARK else ''
for e in ['pdf','svg']: fig.savefig(os.path.join(HERE,f'fig_rq3_dop_load_est_{MODE}{SUF}.'+e),dpi=300)
V=pd.DataFrame(rows).drop_duplicates(['application','system','rate','metric'])
V.to_csv(os.path.join(HERE,f'rq3_dop_load_est_{MODE}_values.csv'),index=False,float_format='%.6g')
used={(a,s,r,m) for a,s,r,m in V[V.source=='estimated'][['application','system','rate','metric']].itertuples(index=False)}
unused=[k for k in EST if k[2] in RATES and k not in used]
print(MODE,'estimated cells used:',len(used),'| conflicts (measured kept):',conflicts,'| unused estimates:',unused,'| n/a cells:',int((V.source=='none').sum()))
