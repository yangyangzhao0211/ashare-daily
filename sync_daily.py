#!/usr/bin/env python3
import os,re,json,time,datetime as dt
from concurrent.futures import ThreadPoolExecutor,as_completed
import akshare as ak
import pandas as pd

ROOT=os.path.dirname(os.path.abspath(__file__)); DATA=os.path.join(ROOT,'data'); DAILY=os.path.join(DATA,'daily')
UNIVERSE=os.path.join(DATA,'universe.csv'); DELIST=os.path.join(DATA,'delist.csv'); META=os.path.join(DATA,'meta.json'); STATE=os.path.join(DATA,'state_v4.json'); FAILED=os.path.join(DATA,'_failed.txt')
START_YEAR=int(os.getenv('START_YEAR','2019')); MODE=os.getenv('MODE','recent').lower(); RECENT_DAYS=int(os.getenv('RECENT_DAYS','130')); WORKERS=int(os.getenv('MAX_WORKERS','8')); BATCH=int(os.getenv('BATCH_SIZE','200')); RETRIES=int(os.getenv('RETRIES','3')); YEARS_PER_RUN=int(os.getenv('HISTORY_YEARS_PER_RUN','1'))
os.makedirs(DAILY,exist_ok=True)
COLS=['date','code','name','open','high','low','close','volume','amount','turnover','pct_chg','src']

def load(path,default):
    try:
        with open(path,encoding='utf-8') as f:return json.load(f)
    except:return default

def save(path,obj):
    tmp=path+'.tmp'; open(tmp,'w',encoding='utf-8').write(json.dumps(obj,indent=2,ensure_ascii=False)); os.replace(tmp,path)

def code(x):
    m=re.search(r'(\d{6})',str(x or '')); return m.group(1) if m else ''

def universe():
    try:
        x=ak.stock_zh_a_spot_em()[['代码','名称']].copy(); x.columns=['code','name']; x.code=x.code.map(code); x=x[x.code!=''].drop_duplicates('code')
        if len(x)<4500: raise RuntimeError('incomplete EM universe')
        print('universe: Eastmoney',len(x),flush=True)
    except Exception:
        x=ak.stock_info_a_code_name().copy(); x['code']=x['code'].map(code); x=x[['code','name']].drop_duplicates('code'); print('universe: AkShare fallback',len(x),flush=True)
    x['is_st']=x.name.fillna('').str.match(r'^\*?ST'); x.to_csv(UNIVERSE,index=False); return x

def delisted():
    fs=[]
    try:
        x=ak.stock_info_sh_delist('全部').rename(columns={'公司代码':'code','公司简称':'name','上市日期':'list_date','暂停上市日期':'delist_date'})
        for c in ['name','list_date','delist_date']:
            if c not in x:x[c]=None
        fs.append(x[['code','name','list_date','delist_date']])
    except Exception as e: print('SH delist failed',e)
    try:
        x=ak.stock_info_sz_delist('终止上市公司'); r={}
        for c in x.columns:
            s=str(c)
            if '代码' in s:r[c]='code'
            elif '简称' in s or '名称' in s:r[c]='name'
            elif '上市日期' in s:r[c]='list_date'
            elif '日期' in s and ('终止' in s or '暂停' in s):r[c]='delist_date'
        x=x.rename(columns=r)
        for c in ['code','name','list_date','delist_date']:
            if c not in x:x[c]=None
        fs.append(x[['code','name','list_date','delist_date']])
    except Exception as e: print('SZ delist failed',e)
    if not fs:return pd.DataFrame(columns=['code','name','list_date','delist_date'])
    x=pd.concat(fs,ignore_index=True); x.code=x.code.map(code); x=x[x.code!=''].drop_duplicates('code'); x.list_date=pd.to_datetime(x.list_date,errors='coerce'); x.delist_date=pd.to_datetime(x.delist_date,errors='coerce'); x.to_csv(DELIST,index=False); return x

def em(c,s,e):
    x=ak.stock_zh_a_hist(symbol=c,period='daily',start_date=s,end_date=e,adjust='')
    if x is None or x.empty:return None
    x=x.rename(columns={'日期':'date','开盘':'open','收盘':'close','最高':'high','最低':'low','成交量':'volume','成交额':'amount','换手率':'turnover','涨跌幅':'pct_chg'})
    need=['date','open','high','low','close','volume','amount','turnover','pct_chg']
    if any(c not in x for c in need):return None
    x['date']=pd.to_datetime(x.date); x['code']=c; x['src']='em'; return x[COLS]

def tx(c,s,e):
    x=ak.stock_zh_a_hist_tx(symbol=('sh' if c.startswith(('6','9')) else 'sz')+c,start_date=s,end_date=e,adjust='')
    if x is None or x.empty:return None
    x['date']=pd.to_datetime(x.date); x=x[(x.date>=pd.Timestamp(s))&(x.date<=pd.Timestamp(e))].copy()
    if x.empty:return None
    x['code']=c; x['volume']=pd.to_numeric(x.volume,errors='coerce')/100; x['amount']=pd.to_numeric(x.close,errors='coerce')*x.volume*100; x['turnover']=float('nan'); x['pct_chg']=pd.to_numeric(x.close,errors='coerce').pct_change()*100; x['src']='tx'
    return x[COLS]

def fetch(c,s,e):
    err=None
    for i in range(RETRIES):
        try:return 'OK',em(c,s,e)
        except Exception as ex:err=ex; time.sleep(1.5*(i+1))
    try:return 'OK',tx(c,s,e)
    except Exception as ex:return 'FAILED',f'em={err}; tx={ex}'

def threshold(c,st,d):
    d=pd.Timestamp(d).date()
    if st:return 5
    if c.startswith('688'):return 20
    if c.startswith(('300','301')):return 20 if d>=dt.date(2020,8,24) else 10
    if c.startswith('8'):return 30 if d>=dt.date(2021,11,15) else 10
    return 10

def flags(x,stmap):
    x=x.copy(); st=x.code.map(stmap).fillna(False); t=[threshold(c,s,d) for c,s,d in zip(x.code,st,x.date)]; t=pd.Series(t,index=x.index); x['limit_up_approx']=x.pct_chg>=t-.5; x['limit_down_approx']=x.pct_chg<=-t+.5; return x

def upsert(x,stmap):
    if x is None or x.empty:return
    for y,g in x.groupby(pd.to_datetime(x.date).dt.year):
        p=os.path.join(DAILY,f'{int(y)}.parquet'); g=flags(g,stmap)
        if os.path.exists(p):g=pd.concat([pd.read_parquet(p),g],ignore_index=True)
        g.date=pd.to_datetime(g.date); g=g.drop_duplicates(['date','code'],keep='last').sort_values(['date','code']); g.to_parquet(p,index=False)

def write_meta():
    m={'years':{},'total_rows':0}
    for fn in os.listdir(DAILY):
        if not fn.endswith('.parquet'):continue
        try:
            y=fn[:4]; x=pd.read_parquet(os.path.join(DAILY,fn)); d=pd.to_datetime(x.date); m['years'][y]={'rows':len(x),'stocks':x.code.nunique(),'date_min':str(d.min().date()),'date_max':str(d.max().date())}; m['total_rows']+=len(x)
        except Exception as e:print('meta',fn,e)
    if m['years']:m['date_min']=min(v['date_min'] for v in m['years'].values()); m['date_max']=max(v['date_max'] for v in m['years'].values())
    save(META,m)

def process(codes,s,e,state,namemap,stmap,label):
    failed=[]
    for off in range(0,len(codes),BATCH):
        bs=codes[off:off+BATCH]; frames=[]; print(f'{label} batch {off+1}-{off+len(bs)}/{len(codes)}',flush=True)
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            fut={ex.submit(fetch,c,s,e):c for c in bs}
            for i,f in enumerate(as_completed(fut),1):
                c=fut[f]
                try:status,r=f.result()
                except Exception as exx:status,r='FAILED',str(exx)
                if status=='FAILED':failed.append(c); state['failed'][c]=r
                else:
                    state['failed'].pop(c,None)
                    if r is not None and not r.empty:frames.append(r)
                if i%50==0 or i==len(bs):print(f'  {i}/{len(bs)}',flush=True)
        if frames:
            x=pd.concat(frames,ignore_index=True); x['name']=x.code.map(namemap); upsert(x,stmap)
        save(STATE,state); open(FAILED,'w').write('\n'.join(sorted(state['failed'])) or '(none)')
    return failed

def recent(uni,state,namemap,stmap):
    end=dt.date.today(); start=end-dt.timedelta(days=RECENT_DAYS); print('='*60); print('RECENT',start,end,flush=True)
    process(sorted(uni.code.astype(str)),start.strftime('%Y%m%d'),end.strftime('%Y%m%d'),state,namemap,stmap,'recent'); state['recent']['last_attempt_date']=str(end); save(STATE,state)

def eligible(uni,dl,y):
    cs=set(uni.code.astype(str));
    if not dl.empty:
        a=pd.Timestamp(f'{y}-01-01');b=pd.Timestamp(f'{y}-12-31'); q=dl[(dl.list_date.isna()| (dl.list_date<=b))&(dl.delist_date.isna()|(dl.delist_date>=a))];cs.update(q.code.astype(str))
    return sorted(cs)

def history(uni,dl,state,namemap,stmap):
    yrs=0
    for y in range(START_YEAR,dt.date.today().year+1):
        if yrs>=YEARS_PER_RUN:break
        ys=state['history']['years'].setdefault(str(y),{'done':[],'completed':False})
        if ys['completed']:continue
        cs=eligible(uni,dl,y); done=set(ys.get('done',[])); rem=[c for c in cs if c not in done]
        print('='*60);print(f'HISTORY {y}: total={len(cs)} remaining={len(rem)}',flush=True)
        for off in range(0,len(rem),BATCH):
            bs=rem[off:off+BATCH]; frames=[]; bad=[]
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                fut={ex.submit(fetch,c,f'{y}0101',f'{y}1231'):c for c in bs}
                for i,f in enumerate(as_completed(fut),1):
                    c=fut[f]
                    try:status,r=f.result()
                    except Exception:status,r='FAILED','exception'
                    if status=='OK':
                        done.add(c)
                        if r is not None and not r.empty:frames.append(r)
                    else:bad.append(c);state['failed'][c]=r
                    if i%50==0 or i==len(bs):print(f'  {i}/{len(bs)} resolved={len(done)} failed={len(bad)}',flush=True)
            if frames:
                x=pd.concat(frames,ignore_index=True);x['name']=x.code.map(namemap);upsert(x,stmap)
            ys['done']=sorted(done);ys['failed']=sorted(bad);save(STATE,state);open(FAILED,'w').write('\n'.join(sorted(state['failed'])) or '(none)')
        if len(done)==len(cs):ys['completed']=True;print(y,'COMPLETE',flush=True)
        else:print(y,'incomplete; resume next history run',flush=True)
        yrs+=1

def main():
    print('A-share daily database V4.0',dt.date.today(),'MODE=',MODE,flush=True)
    u=universe();dl=delisted(); nm=dict(zip(u.code,u.name)); sm=dict(zip(u.code,u.is_st));
    for _,r in dl.iterrows():nm.setdefault(str(r.code),r.get('name',''));sm.setdefault(str(r.code),bool(re.match(r'^\*?ST',str(r.get('name','')))))
    state=load(STATE,{'recent':{},'history':{'years':{}},'failed':{}})
    if not isinstance(state.get('failed'),dict):state['failed']={}
    state.setdefault('recent',{});state.setdefault('history',{'years':{}});state['history'].setdefault('years',{})
    if MODE=='recent':recent(u,state,nm,sm)
    elif MODE=='history':history(u,dl,state,nm,sm)
    elif MODE=='both':recent(u,state,nm,sm);history(u,dl,state,nm,sm)
    else:raise ValueError('MODE must be recent/history/both')
    write_meta();save(STATE,state);print('V4 DONE',flush=True)
if __name__=='__main__':main()
