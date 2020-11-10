import os
from lib.utils import sql_from_cache
import pandas as pd
import numpy as np

def course_enrols(cid):
    sql = '''select distinct shortname as role,userid, firstname, lastname, instanceid as courseid 
        from mdl_context c 
        join mdl_role_assignments ra on c.id=ra.contextid and contextlevel = 50 
        join mdl_role r on r.id = ra.roleid 
        join mdl_user u on u.id = ra.userid and instanceid = {0}'''.format(cid)
    df = sql_from_cache(sql)
    e = df.groupby(['role'])['courseid'].count()
    roles = ['student','editingteacher','advancedteacher','monitor','teacher']
    for role in roles:
        if role not in e.keys():
            e[role] = 0
    e['total'] = len(df['userid'].unique())
    return e

def highestrole(s):
    roles = ['manager','advancedteacher','editingteacher','monitor','teacher','student','guest']
    for role in roles:
        if role in s.array:
            return role
    return

def userid_role(cid):
    'returns array of userid and highest role'
    sql = '''select distinct shortname as role,userid 
        from mdl_context c 
        join mdl_role_assignments ra on c.id=ra.contextid and contextlevel = 50 
        join mdl_role r on r.id = ra.roleid 
        join mdl_user u on u.id = ra.userid and instanceid = {0}'''.format(cid)
    df = sql_from_cache(sql)
    df = df.groupby('userid').agg({'role':highestrole})
    return df

def course_modules(cid):
    sql = '''select course, name, cm.id cmid
         from mdl_course_modules cm 
         join mdl_modules m on cm.module = m.id
         where course = {0}'''.format(cid)
    df = sql_from_cache(sql)
    return df

def course_views(cid):
    sql = '''SELECT contextinstanceid as cmid, COUNT('x') AS views, COUNT(DISTINCT userid) AS uniqusers
              FROM mdl_logstore_standard_log
               WHERE courseid = {0}
               AND anonymous = 0
               AND crud = 'r'
               AND contextlevel = 70
               GROUP BY contextinstanceid'''.format(cid)
    df = sql_from_cache(sql)
    df['courseid'] = cid
    df['cmid'] = df['cmid'].astype('int64')
    df['vpu'] = df['views']/df['uniqusers']
    return df

def nusp2userid(nusps):
    sql = "select id,idnumber from mdl_user where idnumber in ({0})".format(','.join(str(i) for i in nusps))
    return sql_from_cache(sql)

def modules_views(cid):
    m = course_modules(cid)
    v = course_views(cid)
    df = pd.merge(m,v,how='left',left_on=['course','cmid'],right_on=['courseid','cmid']).fillna(0)
    df.drop('courseid',axis=1)
    enrols = course_enrols(cid)
    students = enrols['student']
    teachers = sum([n for role,n in enrols.iteritems() if role in ['editingteacher','advancedteacher','monitor','teacher']])
    df['students'] = students
    df['teachers'] = teachers
    if students > 0:
        df['ups'] = df['uniqusers'] / students
        df['vps'] = df['views'] / students
    else:
        df['ups'] = np.NaN
        df['vps'] = np.NaN
    return df

def course_stats(cid):
    sql = '''
    '''.format(cid)
    
    return
    
def course_data(cid):
    ur = userid_role(cid)
    ur.to_csv('data/processed/userid-role-{0}.csv'.format(cid))

def clean_grades(fn):
    notas = pd.read_csv(fn)
    notas.rename(columns = {'Número USP':'idnumber'},inplace=True)
    notas = pd.merge(notas,nusp2userid(notas['idnumber']),on='idnumber')
    notas.drop(columns = ['Nome','Sobrenome','Instituição','Departamento','Endereço de email','idnumber'],inplace=True)
    notas.rename(columns={'id':'userid'},inplace=True)
    root,ext = os.path.splitext(fn)
    fn = root + '-cleaned' + ext
    notas.to_csv(fn,index=False)
    return notas
    
def clean_grades_jupiter(fn):
    notas = pd.read_csv(fn,sep='\t')    
    # codpes is an object
    notas['codpes'] = notas['codpes'].astype('object')
    # deduplicate codpes, only taking last altered date
    notas.sort_values('dtaultalt',ascending=True,inplace=True)
    notas.drop_duplicates(subset=['codpes'],keep='last',inplace=True)
    notas.rename(columns = {'codpes':'idnumber'},inplace=True)
    notas = pd.merge(notas,nusp2userid(notas['idnumber']),on='idnumber')
    notas = notas[['id','notfim','notfim2','frqfim','rstfim']]
    notas.rename(columns={'id':'userid'},inplace=True)
    root,ext = os.path.splitext(fn)
    fn = root + '-cleaned' + ext
    notas.to_csv(fn,index=False)
    return notas