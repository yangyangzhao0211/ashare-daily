#!/usr/bin/env python3
import json, os, re, sys, time, random, datetime as dt, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local
import requests
import pandas as pd

START_YEAR=2019
MAX_WORKERS=4
BATCH_SIZE=500
REQUEST_TIMEOUT=12
RETRIES=3
EM_KLINE_URL='https://push2his.eastmoney.com/api/qt/stock/kline/get'
EM_LIST_URL='https://push2.eastmoney.com/api/qt/clist/get'
EM_UT='fa5fd1943c7b386f172d6893dbfba10b'
A_SHARE_FS='m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048'
HEADERS={'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/128.0 Safari/537.36','Referer':'https://quote.eastmoney.com/','Accept':'application/json, text/plain, */*'}
TL=local()

def root():
    d=os.path.dirname(os.path.abspath(__file__))
    for _ in range(6):
        if os.path.isdir(os.path.join(d,'.github')): return d
        d=os.path.dirname(d)
    return os.getcwd()
ROOT=root(); DATA=os.path.join(ROOT,'data'); DAILY=os.path.join(DATA,'daily'); STATE=os.path.join(DATA,'state')
UNIVERSE=os.path.join(DATA,'universe.csv'); DELIST=os.path.join(DATA,'delist.csv'); META=os.path.join(DATA,'meta.json'); FAILED=os.path.join(DATA,'_failed.txt')
os.makedirs(DAILY,exist_ok=True); os.makedirs(STATE,exist_ok=True)

def session():
    if not hasattr(TL,'s'):
        TL.s=requests.Session(); TL.s.headers.update(HEADERS)
    return TL.s

def norm(x):
    m=re.search(r'(\d{6})',str(x or '')); return m.group(1) if m else ''
def is_st(n): return bool(re.match(r'^\*?ST',str(n or '')))
def secid(code): return ('1' if str(code).startswith(('6','9')) else '0')+'.'+str(code).zfill(6)

def em_universe():
    rows=[]; pz=5000
    for pn in range(1,10):
        params={'pn':pn,'pz':pz,'po':1,'np':1,'ut':EM_UT,'fltt':2,'invt':2,'fid':'f3','fs':A_SHARE_FS,'fields':'f12,f14'}
        last=None
        for a in range(RETRIES):
            try:
                time.sleep(.2+random.random()*.2)
                r=session().get(EM_LIST_URL,params=params,timeout=REQUEST_TIMEOUT); r.raise_for_status()
                diff=((r.json().get('data') or {}).get('diff')) or []
                if not diff: return pd.DataFrame(rows,columns=['code','name','is_st']) if rows else None
                for x in diff:
                    c=norm(x.get('f12')); n=str(x.get('f14') or '')
                    if c: rows.append((c,n,is_st(n)))
                if len(diff)<pz: return pd.DataFrame(rows,columns=['code','name','is_st']).drop_duplicates('code').sort_values('code').reset_index(drop=True)
                break
            except Exception as e:
                last=e; time.sleep(2**a)
        if last is not None and pn>1: raise last
    return pd.DataFrame(rows,columns=['code','name','is_st']).drop_duplicates('code')

def ak_universe():
    script=r'''import akshare as ak, json, pandas as pd
for fn in ["stock_zh_a_spot_tx","stock_zh_a_spot_em","stock_info_a_code_name"]:
  try:
    df=getattr(ak,fn)()
    if df is None or df.empty: continue
    if fn=="stock_zh_a_spot_tx":
      code=df["code"].astype(str).str.extract(r"(\d{6})")[0]; name=df.get("name",code)
    elif fn=="stock_zh_a_spot_em":
      code=df["代码"].astype(str); name=df["名称"]
    else:
      code=df["code"].astype(str); name=df["name"]
    out=pd.DataFrame({"code":code,"name":name.astype(str)})
    out=out.dropna(subset=["code"]); out["is_st"]=out["name"].map(lambda x: bool(__import__('re').match(r'^\\*?ST',str(x))))
    out=out.drop_duplicates("code")
    if len(out)>=1000: print(out.to_json(orient="records",force_ascii=False)); break
  except Exception as e: pass
'''
    try:
        p=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,timeout=120)
        lines=[x for x in p.stdout.splitlines() if x.strip()]
        if lines:
            d=pd.DataFrame(json.loads(lines[-1]));
            if len(d)>=1000: return d[['code','name','is_st']]
    except Exception: pass
    return None

def get_universe():
    try:
        d=em_universe()
        if d is not None and len(d)>=1000:
            print(f'universe: Eastmoney {len(d)}'); return d
        print('Eastmoney universe incomplete; falling back')
    except Exception as e: print('Eastmoney universe failed:',e)
    d=ak_universe()
    if d is not None:
        print(f'universe: AkShare fallback {len(d)}'); return d
    if os.path.exists(UNIVERSE):
        d=pd.read_csv(UNIVERSE,dtype={'code':str}); d['code']=d['code'].map(norm); d=d.dropna(subset=['code']).drop_duplicates('code')
        if len(d)>=1000:
            print(f'universe: repository fallback {len(d)}'); return d[['code','name','is_st']]
    raise RuntimeError('Cannot obtain a reliable A-share universe (all sources failed)')

def get_delisted():
    script=r'''import akshare as ak,pandas as pd
frames=[]
try:
 sh=ak.stock_info_sh_delist("全部"); sh=sh.rename(columns={"公司代码":"code","公司简称":"name","上市日期":"list_date","暂停上市日期":"delist_date"}); frames.append(sh[[c for c in ['code','name','list_date','delist_date'] if c in sh.columns]])
except: pass
try:
 sz=ak.stock_info_sz_delist("终止上市公司"); rn={}
 for c in sz.columns:
  s=str(c)
  if '代码' in s: rn[c]='code'
  elif '简称' in s or '名称' in s: rn[c]='name'
  elif '上市日期' in s: rn[c]='list_date'
  elif '终止' in s and '日期' in s: rn[c]='delist_date'
 sz=sz.rename(columns=rn); frames.append(sz[[c for c in ['code','name','list_date','delist_date'] if c in sz.columns]])
except: pass
if frames:
 d=pd.concat(frames,ignore_index=True)
 for c in ['code','name','list_date','delist_date']:
  if c not in d: d[c]=None
 d['code']=d['code'].astype(str).str.extract(r'(\d{6})')[0]; d=d.dropna(subset=['code']).drop_duplicates('code')
 for c in ['list_date','delist_date']: d[c]=pd.to_datetime(d[c],errors='coerce')
 print(d.to_json(orient='records',force_ascii=False))
else: print('[]')
'''
    try:
        p=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,timeout=120)
        lines=[x for x in p.stdout.splitlines() if x.strip()]
        if lines: return pd.DataFrame(json.loads(lines[-1]))
    except Exception as e: print('delisted lookup failed:',e)
    return pd.DataFrame(columns=['code','name','list_date','delist_date'])

def fetch_one(code,start,end):
    params={'secid':secid(code),'ut':EM_UT,'fields1':'f1,f2,f3,f4,f5,f6','fields2':'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61','klt':101,'fqt':0,'beg':start,'end':end}
    last=''
    for a in range(RETRIES):
        try:
            time.sleep(.15+random.random()*.15)
            r=session().get(EM_KLINE_URL,params=params,timeout=REQUEST_TIMEOUT); r.raise_for_status(); kl=((r.json().get('data') or {}).get('klines')) or []
            if not kl: return 'EMPTY',None
            rows=[]
            for line in kl:
                p=str(line).split(',')
                if len(p)>=11: rows.append([p[0],code,p[1],p[2],p[3],p[4],p[5],p[6],p[10],p[8]])
            if not rows: return 'EMPTY',None
            d=pd.DataFrame(rows,columns=['date','code','open','close','high','low','volume','amount','turnover','pct_chg'])
            d['date']=pd.to_datetime(d['date'],errors='coerce')
            for c in ['open','close','high','low','volume','amount','turnover','pct_chg']: d[c]=pd.to_numeric(d[c],errors='coerce')
            d['src']='em'; return 'OK',d
        except Exception as e:
            last=str(e); time.sleep(1.5*(2**a)+random.random())
    return 'FAILED',last

def state_path(y): return os.path.join(STATE,f'{y}.json')
def load_state(y):
    p=state_path(y)
    if os.path.exists(p):
        try:
            s=json.load(open(p,encoding='utf-8')); s.setdefault('done',[]); s.setdefault('failed',{}); return s
        except: pass
    return {'year':y,'done':[],'failed':{}}
def save_state(y,s):
    tmp=state_path(y)+'.tmp'; json.dump(s,open(tmp,'w',encoding='utf-8'),ensure_ascii=False,indent=2); os.replace(tmp,state_path(y))

def limit_thr(code,st,date):
    d=pd.Timestamp(date).date()
    if st:return 5
    if code.startswith('688'):return 20
    if code[:3] in ('300','301'):return 20 if d>=dt.date(2020,8,24) else 10
    if code.startswith('8'):return 30 if d>=dt.date(2021,11,15) else 10
    return 10

def add_flags(d,stmap):
    d=d.copy(); d['is_st']=d['code'].map(stmap).fillna(False); th=[limit_thr(c,s,x) for c,s,x in zip(d.code,d.is_st,d.date)]; d['limit_up_approx']=d.pct_chg >= pd.Series(th,index=d.index)-.5; d['limit_down_approx']=d.pct_chg <= -(pd.Series(th,index=d.index)-.5); return d.drop(columns='is_st')

def merge_year(y,new,stmap):
    p=os.path.join(DAILY,f'{y}.parquet')
    if new is None or new.empty: return
    new=add_flags(new,stmap)
    if os.path.exists(p): old=pd.read_parquet(p); d=pd.concat([old,new],ignore_index=True)
    else: d=new
    d=d.drop_duplicates(['date','code'],keep='last').sort_values(['date','code']).reset_index(drop=True)
    tmp=p+'.tmp'; d.to_parquet(tmp,index=False); os.replace(tmp,p)

def git_checkpoint(label):
    try:
        subprocess.run(['git','config','user.name','ashare-bot'],check=True)
        subprocess.run(['git','config','user.email','ashare-bot@users.noreply.github.com'],check=True)
        subprocess.run(['git','add','data/'],check=True)
        q=subprocess.run(['git','diff','--cached','--quiet'])
        if q.returncode==0: return
        subprocess.run(['git','commit','-m',label],check=True)
        subprocess.run(['git','push'],check=True)
        print('checkpoint pushed:',label)
    except Exception as e:
        print('checkpoint push failed; continuing:',e)

def process_year(y,uni,dl,stmap,namemap):
    state=load_state(y); done=set(state['done'])
    active=list(uni.code.astype(str))
    if not dl.empty:
        for _,r in dl.iterrows():
            ld=r.get('list_date'); dd=r.get('delist_date')
            if pd.isna(ld) or pd.Timestamp(ld)<=pd.Timestamp(f'{y}-12-31'):
                if pd.isna(dd) or pd.Timestamp(dd)>=pd.Timestamp(f'{y}-01-01'): active.append(str(r.code).zfill(6))
    active=sorted(set(active)); remaining=[c for c in active if c not in done]
    print(f'{y}: total={len(active)} remaining={len(remaining)}')
    if not remaining:
        return True
    start=f'{y}0101'; end=f'{y}1231'; failed=[]
    for i in range(0,len(remaining),BATCH_SIZE):
        batch=remaining[i:i+BATCH_SIZE]; frames=[]
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut={ex.submit(fetch_one,c,start,end):c for c in batch}
            for n,f in enumerate(as_completed(fut),1):
                c=fut[f]
                try: status,val=f.result()
                except Exception as e: status,val='FAILED',str(e)
                if status=='OK': frames.append(val)
                elif status=='FAILED': failed.append(c); state['failed'][c]=val
                done.add(c)
                if n%100==0: print(f'  {y}: {i+n}/{len(remaining)}')
        if frames:
            new=pd.concat(frames,ignore_index=True); new['name']=new.code.map(namemap).fillna(new.code); merge_year(y,new,stmap)
        for c in failed: done.discard(c)
        state['done']=sorted(done); state['complete']=False; save_state(y,state); git_checkpoint(f'checkpoint {y} {min(i+BATCH_SIZE,len(remaining))}/{len(remaining)}')
    # failed stocks are removed from done so next run retries them
    for c in failed: done.discard(c)
    state['done']=sorted(done); state['failed_count']=len(failed); state['complete']=(len(done)>=len(active)); save_state(y,state)
    complete=state['complete']
    if complete: git_checkpoint(f'complete {y}')
    return complete

def main():
    today=dt.date.today(); uni=get_universe(); uni.to_csv(UNIVERSE,index=False)
    stmap=dict(zip(uni.code,uni.is_st)); namemap=dict(zip(uni.code,uni.name))
    dl=get_delisted(); dl.to_csv(DELIST,index=False)
    for _,r in dl.iterrows():
        c=str(r.code).zfill(6); stmap.setdefault(c,is_st(r.get('name',''))); namemap.setdefault(c,r.get('name',''))
    # historical backfill: resume from first incomplete year
    for y in range(START_YEAR,today.year+1):
        if not process_year(y,uni,dl,stmap,namemap):
            print(f'stopping after {y}; next run will resume from checkpoint'); break
    # incremental only when all historical years are complete
    all_complete=all(os.path.exists(state_path(y)) and load_state(y).get('complete') is True for y in range(START_YEAR,today.year+1))
    if all_complete:
        last=None
        if os.path.exists(META):
            try: last=pd.to_datetime(json.load(open(META)).get('date_max')).date()
            except: pass
        if last is None: last=today-dt.timedelta(days=7)
        if last<today:
            s=(last+dt.timedelta(days=1)).strftime('%Y%m%d'); e=today.strftime('%Y%m%d'); frames=[]; failed=[]
            codes=sorted(uni.code.astype(str))
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                fut={ex.submit(fetch_one,c,s,e):c for c in codes}
                for f in as_completed(fut):
                    c=fut[f]
                    try: status,val=f.result()
                    except Exception as e2: status,val='FAILED',str(e2)
                    if status=='OK': frames.append(val)
                    elif status=='FAILED': failed.append(c)
            if frames:
                new=pd.concat(frames,ignore_index=True); new['name']=new.code.map(namemap).fillna(new.code)
                for y,g in new.groupby(new.date.dt.year): merge_year(int(y),g,stmap)
                json.dump({'date_max':str(new.date.max().date())},open(META,'w'),indent=2)
            else: json.dump({'date_max':str(last)},open(META,'w'),indent=2)
            open(FAILED,'w').write('\n'.join(sorted(failed)) or '(none)')
            git_checkpoint(f'daily sync {today.isoformat()}')
    else: print('historical backfill incomplete; incremental sync deferred')

if __name__=='__main__': main()
