"""Mock-ups of three compact alternatives to Fig. 8 (RQ3 Pareto).
Data: datasets_v2/real_query_level.csv, m510, uniform/uniform_1/uniform_2, no folder_flag,
DoP = round(parallelism_avg) in {2,4,8,16,32,40}. Variant per application: Ad Analytics = with_join,
otherwise the variant with most uniform runs (ties alphabetical). Per (app, system, rate, DoP):
median throughput_50 and median endtoend_latency_ms over runs; cells with throughput<=0 or latency<=0 dropped.
Efficient (Pareto) DoP: no other DoP has throughput >= and latency <= with one strict."""
import os, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
plt.rcParams.update({'font.size':7,'font.family':'serif','axes.linewidth':0.5,'xtick.major.width':0.5,'ytick.major.width':0.5})
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&R.strategy_folder.isin(['uniform','uniform_1','uniform_2'])&(R.folder_flag.fillna('')=='')].copy()
R['variant']=R.variant.fillna(''); R['dop']=R.parallelism_avg.round()
DOPS=[2,4,8,16,32,40]; R=R[R.dop.isin(DOPS)]
pick={}
for a,g in R.groupby('application'):
    c=g.groupby('variant').size().sort_index()
    pick[a]='with_join' if a=='Ad Analytics' else c.idxmax()
R=R[R.apply(lambda r: r.variant==pick[r.application],axis=1)]
M=R.groupby(['application','system','producer_rate','dop']).agg(thr=('throughput_50','median'),lat=('endtoend_latency_ms','median')).reset_index()
M=M[(M.thr>0)&(M.lat>0)]
def front(g):
    eff=[]
    for _,r in g.iterrows():
        dom=((g.thr>=r.thr)&(g.lat<=r.lat)&((g.thr>r.thr)|(g.lat<r.lat))).any()
        eff.append(not dom)
    return pd.Series(eff,index=g.index)
M['eff']=M.groupby(['application','system','producer_rate'],group_keys=False).apply(front)
M.to_csv(os.path.join(HERE,'pareto_cells.csv'),index=False)
SYS=['flink','storm','spark','kafkastream','risingwave']; SN={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Str.','risingwave':'RisingWave'}
RATES=[1e5,5e5,1e6]; RC={1e5:'#4CA58A',5e5:'#E3A33B',1e6:'#C8413F'}; RL={1e5:'100k',5e5:'500k',1e6:'1M'}
W4=[('Ad Analytics','AD (State+Net.)'),('Click Analytics','CA (State)'),('Smart Grid','SG (State+UDF)'),('Word Count','WC (Stateless)')]
def save(fig,name):
    for e in ['pdf','png']: fig.savefig(os.path.join(HERE,name+'.'+e),dpi=300,bbox_inches='tight')
# ---------- Option A: 2x2 scatter, Flink & Spark, AD & WC, column width
fig,ax=plt.subplots(2,2,figsize=(3.5,2.6))
for i,(app,lab) in enumerate([W4[0],W4[3]]):
    for j,s in enumerate(['flink','spark']):
        a=ax[i,j]; g=M[(M.application==app)&(M.system==s)]
        for r in RATES:
            h=g[g.producer_rate==r].sort_values('lat')
            if h.empty: continue
            e=h[h.eff].sort_values('lat'); d=h[~h.eff]
            a.plot(e.lat/1e3,e.thr,'-o',color=RC[r],ms=2.5,lw=0.9)
            a.scatter(d.lat/1e3,d.thr,s=8,facecolors='none',edgecolors=RC[r],lw=0.6)
            for _,p in e.iterrows(): a.annotate(int(p.dop),(p.lat/1e3,p.thr),fontsize=5,color=RC[r],xytext=(1.5,1.5),textcoords='offset points')
        a.set_xscale('log'); a.set_yscale('log'); a.grid(alpha=0.25,lw=0.4)
        if i==0: a.set_title(SN[s],fontsize=7.5,fontweight='bold')
        if j==0: a.set_ylabel(lab+'\nthroughput [ev/s]',fontsize=6.5)
        if i==1: a.set_xlabel('E2E latency [s]',fontsize=6.5)
        a.tick_params(labelsize=5.5)
hs=[plt.Line2D([],[],color=RC[r],marker='o',ms=2.5,lw=0.9,label=RL[r]) for r in RATES]
fig.legend(handles=hs,loc='upper center',ncol=3,frameon=False,fontsize=6,bbox_to_anchor=(0.55,1.04))
fig.tight_layout(pad=0.3); save(fig,'optA_pareto_2x2'); plt.close(fig)
# ---------- Option B: efficient-DoP strip, 4 workloads x 5 systems, column width
fig,ax=plt.subplots(1,5,figsize=(3.5,2.0),sharey=True)
yt=[];yl=[]
for k,s in enumerate(SYS):
    a=ax[k]
    for wi,(app,lab) in enumerate(W4):
        for ri,r in enumerate(RATES):
            y=wi*4+ri
            g=M[(M.application==app)&(M.system==s)&(M.producer_rate==r)]
            a.plot([0,5],[y,y],color='#dddddd',lw=0.4,zorder=0)
            if g.empty: a.text(2.5,y,'n/a',fontsize=4,ha='center',va='center',color='#999'); continue
            x=[DOPS.index(d) for d in g[g.eff].dop]; xd=[DOPS.index(d) for d in g[~g.eff].dop]
            a.scatter(xd,[y]*len(xd),s=5,facecolors='none',edgecolors='#bbbbbb',lw=0.4)
            a.scatter(x,[y]*len(x),s=9,color=RC[r],zorder=3)
            if k==0: yt.append(y); yl.append(RL[r])
        if k==0: a.text(-3.6,wi*4+1,lab.split(' ')[0],fontsize=6,fontweight='bold',ha='right',va='center')
    a.set_xticks(range(6)); a.set_xticklabels(DOPS,fontsize=4.5); a.set_title(SN[s],fontsize=6,fontweight='bold')
    a.invert_yaxis(); a.tick_params(length=1.5)
    for sp in ['top','right']: a.spines[sp].set_visible(False)
ax[0].set_yticks(yt); ax[0].set_yticklabels(yl,fontsize=4.5)
fig.text(0.55,-0.02,'DoP (filled: Pareto-efficient, hollow: dominated)',ha='center',fontsize=6)
fig.tight_layout(pad=0.3,w_pad=0.2); save(fig,'optB_efficient_dop_strip'); plt.close(fig)
# ---------- Option B': all 14 workloads, bubble = number of workloads for which a DoP is efficient
fig,ax=plt.subplots(1,5,figsize=(3.5,1.25),sharey=True)
for k,s in enumerate(SYS):
    a=ax[k]
    for ri,r in enumerate(RATES):
        g=M[(M.system==s)&(M.producer_rate==r)]
        n=g.application.nunique()
        for di,d in enumerate(DOPS):
            c=g[(g.dop==d)&g.eff].application.nunique()
            if c: a.scatter(di,ri,s=c*6,color=RC[r],alpha=0.85,lw=0)
        a.text(5.6,ri,f'n={n}',fontsize=4,va='center',color='#777')
    a.set_xticks(range(6)); a.set_xticklabels(DOPS,fontsize=4,rotation=90); a.set_xlim(-0.6,6.4); a.set_ylim(-0.6,2.6)
    a.set_title(SN[s],fontsize=6,fontweight='bold'); a.grid(alpha=0.2,lw=0.3); a.tick_params(length=1.5)
ax[0].set_yticks(range(3)); ax[0].set_yticklabels([RL[r] for r in RATES],fontsize=5)
fig.text(0.55,-0.06,'DoP (bubble size: number of workloads for which this DoP is Pareto-efficient)',ha='center',fontsize=5.5)
fig.tight_layout(pad=0.3,w_pad=0.2); save(fig,'optB2_efficient_dop_bubbles_all14'); plt.close(fig)
# ---------- Option C: table, median smallest/largest efficient DoP over all workloads
rows=[]
for s in SYS:
    row=[SN[s]]
    for r in RATES:
        g=M[(M.system==s)&(M.producer_rate==r)&M.eff]
        mn=g.groupby('application').dop.min().median(); mx=g.groupby('application').dop.max().median()
        row.append(f'{mn:.0f} / {mx:.0f}' if not np.isnan(mn) else '--')
    rows.append(row)
T=pd.DataFrame(rows,columns=['System','100k','500k','1M']); T.to_csv(os.path.join(HERE,'optC_table.csv'),index=False)
fig,a=plt.subplots(figsize=(3.5,1.1)); a.axis('off')
tb=a.table(cellText=T.values,colLabels=['System','100k ev/s','500k ev/s','1M ev/s'],loc='center',cellLoc='center')
tb.auto_set_font_size(False); tb.set_fontsize(6.5); tb.scale(1,1.15)
for (i,j),c in tb.get_celld().items():
    c.set_linewidth(0.3)
    if i==0: c.set_text_props(fontweight='bold')
a.set_title('Median smallest / largest Pareto-efficient DoP over 14 workloads',fontsize=6.5)
save(fig,'optC_table'); plt.close(fig)
print(pick); print(T.to_string(index=False))
