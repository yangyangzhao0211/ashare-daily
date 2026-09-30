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
    """
    Robust A-share universe loader.

    Priority:
    1. Eastmoney current full-market snapshot
    2. AkShare static A-share list
    3. Existing local data/universe.csv

    The local fallback is only accepted if it contains a plausible
    full-market universe (>4500 stocks), so a partial/stale file
    cannot silently corrupt the database.
    """

    # --------------------------------------------------------
    # 1. Eastmoney live universe
    # --------------------------------------------------------
    try:
        x = ak.stock_zh_a_spot_em()[["代码", "名称"]].copy()

        x.columns = ["code", "name"]

        x["code"] = x["code"].map(code)

        x = (
            x[x["code"] != ""]
            .drop_duplicates("code")
            .reset_index(drop=True)
        )

        if len(x) < 4500:
            raise RuntimeError(
                f"incomplete Eastmoney universe: {len(x)} stocks"
            )

        x["is_st"] = (
            x["name"]
            .fillna("")
            .str.match(r"^\*?ST")
        )

        print(
            f"universe: Eastmoney {len(x)} stocks",
            flush=True
        )

        x.to_csv(
            UNIVERSE,
            index=False
        )

        return x

    except Exception as e:
        print(
            f"Eastmoney universe failed: {type(e).__name__}: {e}",
            flush=True
        )

    # --------------------------------------------------------
    # 2. AkShare static universe
    # --------------------------------------------------------
    try:
        x = ak.stock_info_a_code_name().copy()

        x["code"] = x["code"].map(code)

        x = (
            x[["code", "name"]]
            .drop_duplicates("code")
            .reset_index(drop=True)
        )

        x = x[x["code"] != ""]

        if len(x) < 4500:
            raise RuntimeError(
                f"incomplete AkShare universe: {len(x)} stocks"
            )

        x["is_st"] = (
            x["name"]
            .fillna("")
            .str.match(r"^\*?ST")
        )

        print(
            f"universe: AkShare fallback {len(x)} stocks",
            flush=True
        )

        x.to_csv(
            UNIVERSE,
            index=False
        )

        return x

    except Exception as e:
        print(
            f"AkShare universe failed: {type(e).__name__}: {e}",
            flush=True
        )

    # --------------------------------------------------------
    # 3. Local repository fallback
    # --------------------------------------------------------
    try:
        if os.path.exists(UNIVERSE):

            x = pd.read_csv(
                UNIVERSE,
                dtype={"code": str}
            )

            if "code" not in x.columns or "name" not in x.columns:
                raise RuntimeError(
                    "local universe.csv missing code/name columns"
                )

            x["code"] = x["code"].map(code)

            x = (
                x[x["code"] != ""]
                .drop_duplicates("code")
                .reset_index(drop=True)
            )

            if len(x) < 4500:
                raise RuntimeError(
                    f"local universe too small: {len(x)} stocks"
                )

            if "is_st" not in x.columns:
                x["is_st"] = (
                    x["name"]
                    .fillna("")
                    .str.match(r"^\*?ST")
                )

            else:
                x["is_st"] = (
                    x["is_st"]
                    .astype(str)
                    .str.lower()
                    .isin(["true", "1", "yes"])
                )

            print(
                f"universe: local fallback {len(x)} stocks",
                flush=True
            )

            return x[
                ["code", "name", "is_st"]
            ]

        raise RuntimeError(
            "data/universe.csv does not exist"
        )

    except Exception as e:
        print(
            f"Local universe fallback failed: {type(e).__name__}: {e}",
            flush=True
        )

    # --------------------------------------------------------
    # All methods failed
    # --------------------------------------------------------
    raise RuntimeError(
        "Cannot obtain a reliable A-share universe "
        "from Eastmoney, AkShare, or local universe.csv"
    )

def delisted():
    """
    Best-effort delisted-stock list.

    Returns:
        code, name, list_date, delist_date

    Failure of either exchange source is non-fatal.
    This implementation deliberately selects columns instead of
    renaming many columns at once, avoiding duplicate column names.
    """

    frames = []

    # ========================================================
    # Shanghai
    # ========================================================

    try:
        raw = ak.stock_info_sh_delist("全部")

        if raw is not None and not raw.empty:

            def pick_sh(candidates):
                for c in candidates:
                    if c in raw.columns:
                        return raw[c]
                return pd.Series([None] * len(raw))

            sh = pd.DataFrame({
                "code": pick_sh([
                    "公司代码",
                    "证券代码",
                    "股票代码",
                ]),

                "name": pick_sh([
                    "公司简称",
                    "证券简称",
                    "股票简称",
                    "名称",
                ]),

                "list_date": pick_sh([
                    "上市日期",
                ]),

                "delist_date": pick_sh([
                    "终止上市日期",
                    "退市日期",
                    "暂停上市日期",
                ]),
            })

            sh["code"] = sh["code"].map(code)

            sh = sh[
                sh["code"] != ""
            ].drop_duplicates("code")

            frames.append(sh)

            print(
                f"SH delisted: {len(sh)}",
                flush=True
            )

    except Exception as e:
        print(
            f"SH delist failed: "
            f"{type(e).__name__}: {e}",
            flush=True
        )

    # ========================================================
    # Shenzhen
    # ========================================================

    try:
        raw = ak.stock_info_sz_delist(
            "终止上市公司"
        )

        if raw is not None and not raw.empty:

            def find_column(keywords,
                            exclude_keywords=None):

                exclude_keywords = (
                    exclude_keywords or []
                )

                for c in raw.columns:

                    text = str(c)

                    if (
                        all(
                            k in text
                            for k in keywords
                        )
                        and not any(
                            k in text
                            for k in exclude_keywords
                        )
                    ):
                        return c

                return None

            code_col = (
                find_column(["代码"])
            )

            name_col = (
                find_column(["简称"])
                or find_column(["名称"])
            )

            list_col = find_column([
                "上市",
                "日期",
            ])

            delist_col = (
                find_column([
                    "终止",
                    "日期",
                ])
                or find_column([
                    "退市",
                    "日期",
                ])
            )

            sz = pd.DataFrame()

            if code_col is not None:
                sz["code"] = raw[code_col]
            else:
                sz["code"] = None

            if name_col is not None:
                sz["name"] = raw[name_col]
            else:
                sz["name"] = None

            if list_col is not None:
                sz["list_date"] = raw[list_col]
            else:
                sz["list_date"] = None

            if delist_col is not None:
                sz["delist_date"] = raw[delist_col]
            else:
                sz["delist_date"] = None

            sz["code"] = sz["code"].map(code)

            sz = sz[
                sz["code"] != ""
            ].drop_duplicates("code")

            frames.append(sz)

            print(
                f"SZ delisted: {len(sz)}",
                flush=True
            )

    except Exception as e:
        print(
            f"SZ delist failed: "
            f"{type(e).__name__}: {e}",
            flush=True
        )

    # ========================================================
    # Merge
    # ========================================================

    if not frames:

        print(
            "delisted: unavailable; "
            "continuing with empty list",
            flush=True
        )

        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "list_date",
                "delist_date",
            ]
        )

    try:

        x = pd.concat(
            frames,
            ignore_index=True,
            sort=False,
        )

    except Exception as e:

        print(
            f"delisted concat failed: {e}; "
            "continuing with empty list",
            flush=True
        )

        return pd.DataFrame(
            columns=[
                "code",
                "name",
                "list_date",
                "delist_date",
            ]
        )

    # Guarantee unique standardized columns.
    x = x[
        [
            "code",
            "name",
            "list_date",
            "delist_date",
        ]
    ].copy()

    x["code"] = x["code"].map(code)

    x = (
        x[x["code"] != ""]
        .drop_duplicates("code")
        .reset_index(drop=True)
    )

    x["list_date"] = pd.to_datetime(
        x["list_date"],
        errors="coerce",
    )

    x["delist_date"] = pd.to_datetime(
        x["delist_date"],
        errors="coerce",
    )

    try:
        x.to_csv(
            DELIST,
            index=False
        )

    except Exception as e:
        print(
            f"WARNING: cannot save delist.csv: {e}",
            flush=True
        )

    print(
        f"delisted total: {len(x)}",
        flush=True
    )

    return x

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

    print(
        "A-share daily database V4.0",
        dt.date.today(),
        "MODE=",
        MODE,
        flush=True
    )

    # ========================================================
    # Current listed universe
    # ========================================================

    u = universe()

    nm = dict(
        zip(
            u.code,
            u.name
        )
    )

    sm = dict(
        zip(
            u.code,
            u.is_st
        )
    )

    # ========================================================
    # Load state
    # ========================================================

    state = load(
        STATE,
        {
            "recent": {},
            "history": {
                "years": {}
            },
            "failed": {},
        }
    )

    if not isinstance(
        state.get("failed"),
        dict
    ):
        state["failed"] = {}

    state.setdefault(
        "recent",
        {}
    )

    state.setdefault(
        "history",
        {
            "years": {}
        }
    )

    state["history"].setdefault(
        "years",
        {}
    )

    # ========================================================
    # RECENT
    # ========================================================
    #
    # Recent mode does NOT need delisted stocks.
    # Avoid unnecessary fragile network calls.
    # ========================================================

    if MODE == "recent":

        print(
            "recent mode: skipping delisted lookup",
            flush=True
        )

        recent(
            u,
            state,
            nm,
            sm
        )

    # ========================================================
    # HISTORY
    # ========================================================

    elif MODE == "history":

        print(
            "history mode: loading delisted stocks",
            flush=True
        )

        dl = delisted()

        for _, r in dl.iterrows():

            c = str(
                r.code
            )

            nm.setdefault(
                c,
                r.get(
                    "name",
                    ""
                )
            )

            sm.setdefault(
                c,
                bool(
                    re.match(
                        r"^\*?ST",
                        str(
                            r.get(
                                "name",
                                ""
                            )
                        )
                    )
                )
            )

        history(
            u,
            dl,
            state,
            nm,
            sm
        )

    # ========================================================
    # BOTH
    # ========================================================

    elif MODE == "both":

        # Recent first.
        print(
            "both mode: running recent first",
            flush=True
        )

        recent(
            u,
            state,
            nm,
            sm
        )

        # Then history.
        print(
            "both mode: loading delisted stocks",
            flush=True
        )

        dl = delisted()

        for _, r in dl.iterrows():

            c = str(
                r.code
            )

            nm.setdefault(
                c,
                r.get(
                    "name",
                    ""
                )
            )

            sm.setdefault(
                c,
                bool(
                    re.match(
                        r"^\*?ST",
                        str(
                            r.get(
                                "name",
                                ""
                            )
                        )
                    )
                )
            )

        history(
            u,
            dl,
            state,
            nm,
            sm
        )

    else:

        raise ValueError(
            "MODE must be recent/history/both"
        )

    # ========================================================
    # Metadata
    # ========================================================

    write_meta()

    save(
        STATE,
        state
    )

    print(
        "V4 DONE",
        flush=True
    )
if __name__=='__main__':main()
