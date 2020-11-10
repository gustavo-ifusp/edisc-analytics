import hashlib, os
import pandas as pd
from sqlalchemy import create_engine

password = os.environ['DBPASS']

engine = create_engine('mysql+mysqlconnector://edustats:%s@200.144.245.242:60331/moodle_usp' % password, encoding='utf8')

def sql_from_cache(sql,invalidate=False,**kwargs):
    s = sql+str(engine.url)
    h = hashlib.md5(s.encode()).hexdigest()
    fn = '~/edisc-analytics/cache/'+h+'.csv'
    if invalidate:
        try:
            os.remove(fn)
        except FileNotFoundError:
            pass
    try:
        df = pd.read_csv(fn)
        return df
    except FileNotFoundError:
        pass
    df = pd.read_sql_query(sql,engine,**kwargs)
    print(".")
    df.to_csv(fn,index=False)
    return df

def year(dt):
    return dt.year

def sem(dt):
    return (dt.month - 1) // 6 + 1