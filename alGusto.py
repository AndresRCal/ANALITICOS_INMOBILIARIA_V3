# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 12:06:59 2016

@author: AAREYESC
"""
import os
from dateutil import parser
import time
import pandas as pd
import codecs
import cx_Oracle as cx
import sqlite3

fecha=time.strftime("%x")
hora= time.strftime("%X")
os.environ['NLS_LANG'] ='.UTF8'
socket='''crmpdb-scan.puerto.liverpool.com.mx:1527/CRMP'''

def escribe_log(texto,path=os.getcwd(),tema='',extencion='txt'):
    archi=codecs.open(path+os.sep+dar_nombre('log',tema,extencion),'a','utf8')
    archi.write(str(texto))
    archi.write('\r\n')
    return()

def dar_nombre(subtema,tema,extencion):
    nombre=str(subtema)+'_'+str(fecha).replace('/','-')+'_'+str(tema)+'.'+str(extencion)
    return(nombre)

def concat_hyphen(seq):
    p='-'
    chain=p.join(seq)
    return(chain)

def concat_pointsep(seq):
    p='.'
    chain=p.join(seq)
    return(chain)
    
def concat_commasep(seq):
    p=','
    chain=p.join(seq)
    return(chain)
    
def concat_orsep(seq):
    p=' or '
    chain=p.join(seq)
    return(chain)    
    
def MakinSQLconditionList(chain):
    chain=chain.strip('\n').replace('\n',"','")
    chain="'"+chain+"'"
    return(chain)
    
def Wcsv_Wpandas(Dframe,Fname,Path):
    Dframe.to_csv(Path+os.sep+Fname+'.csv',index=False,enconding='latin-1')
    print('Se ha creado el csv',Fname)
    return()
    
def Identifying_TBname(tb_name):
    D=tb_name
    D=D=D.split('.')
    fin=len(D)-1
    return(D[fin])

def asking_data_connection_orcl(cve=''):
    if cve == '':
        cons=pd.read_csv('connection_file.csv')
        return(cons)
    else:
        cons=pd.read_csv('connection_file.csv')
        cons=cons[cons['cve']==cve.upper()]
        return(cons)

def making_connections(cve):
    cve=cve.upper()
    df_aux=asking_data_connection_orcl(cve)
    ind=df_aux.index
    ind_val=0
    if df_aux.empty:
        return(False)
    else:
        for j in ind:
            ind_val=j
        try:
            db=cx.connect(str(df_aux['USUARIO'][ind_val]),str(df_aux['PASSWORD'][ind_val]),str(df_aux['IP_Puerto_SID'][ind_val]))
            return(db)
        except cx.Error as msg:
            print(msg)
            return(False)
        
def run_qry_pd_default(cve,qry):
    db=making_connections(cve)
    if db==False:
        print('Error de conexion no se puede proceder')
        return(False)
        pass
    else:
        try:
            DFrame=pd.read_sql(qry,db,index_col=None)
            DFrame=DFrame.fillna('')
            return(DFrame)
        except Exception as rep:
            msg='Error:',str(db)+str(rep)
            print(msg)
            return(False)
            
def run_sqlite_qry(path,DB_file,qry):
    try:
        conn=sqlite3.connect(path+os.sep+DB_file)
        DF=pd.read_sql(qry,conn,index_col=None)
        return(DF)
    except Exception as msg:
        print(msg)
        return(False)

def describe_oracle_table(cve,tb_name):
    tb_name=Identifying_TBname(tb_name)
    qry_base="""
    select column_name "Campo", data_type "Tipo de dato", 
    data_length "Tamaño", nullable "Permite nulos"
    from all_tab_columns where table_name = '"""
    qry=qry_base+str(tb_name).upper()+"'"
    try:
        desc_tb=run_qry_pd_default(cve,qry)
        return(desc_tb)
    except cx.Error as msg:
        print(msg)
    
def forming_colmn_LOB_Varchar(Campo,dtype):
    if dtype not in['CLOB','LOB','BLOB']:
        val_new_column=Campo
    else:
        val_new_column='''SYS.DBMS_LOB.SUBSTR('''+Campo+''',4000,1) as '''+Campo
    return(val_new_column)
    
def FormingQuery_CLOB_table_toDF(cve,tb_name):
    lst_vals=[]
    select=''
    desc_tb=describe_oracle_table(cve,tb_name)
    indx=desc_tb.index
    indx=list(indx)
    for j in indx:
        lst_vals+=[forming_colmn_LOB_Varchar(desc_tb['Campo'][j],desc_tb['Tipo de dato'][j])]
    fin=len(lst_vals)-1
    band=0
    for k in lst_vals:
        if band == fin:
            select+=k
        else:
            select+=k+',\n'
        band=band+1
    qry='SELECT '+select+' FROM '+str(tb_name).upper()
    return(qry.replace("SELECT  FROM","SELECT * FROM"))
    
def search_tableOracle(cve,tb_name):
    qry='''select owner, table_name from all_tables where table_name like upper('%'''+tb_name+"""%') order by owner, table_name"""
    DFrame=run_qry_pd_default(cve,qry)
    if DFrame.empty:
        return(False)
    else:
        indx=DFrame.index
        indx=list(indx)
        new_vals=[]
        for i in indx:
            seq=(str(DFrame['OWNER'][i]),str(DFrame['TABLE_NAME'][i]))
            new_vals+=[concat_pointsep(seq)]
        DFrame['Opc_2']=new_vals
        return(DFrame)
    
def orcltable_to_dataframe(cve,tb_name):
    qry=FormingQuery_CLOB_table_toDF(cve,tb_name)
    DFrame=run_qry_pd_default(cve,qry)
    return(DFrame)
    
def truncateTableOracle(cve,tb_name):
    qry='''truncate table '''+tb_name
    db=making_connections(cve)
    c=db.cursor()
    try:
        c.execute(qry)
        print('Se ha truncado la tabla:',tb_name)
    except cx.Error as msg:
        print(msg)
    
def Rcsv_Wpandas(NFile,path):
    try:
        DFrame=pd.read_csv(path+os.sep+NFile,encoding='latin_1')
    except Exception as ins:
        DFrame=pd.read_csv(path+os.sep+NFile)
        print(ins)
        print('Se leera con encoding automatico')
    return(DFrame)
    
def Dframe_to_sqlitehome(path,DBfile,tb_name,mode,DFrame):
    mode_aux=mode
    while mode_aux not in ['w','a']:
        mode_aux=input("""mode: solo acepta las opciones: 
            'w' para en caso de existir contenido hacer un replace
            'a' para en caso de existir contenido hacer un append
                
                Favor de insertar un valor valido
                
                """)
    dir_1=os.getcwd()
    dir_2=path
    os.chdir(dir_2)
    with sqlite3.connect(DBfile) as cnx:
        try:
            if mode_aux == 'w':
                DFrame.to_sql(tb_name, cnx, if_exists='replace',encoding='latin_1')
            else:
                DFrame.to_sql(tb_name, cnx, if_exists='append',encoding='latin_1')
        except Exception as msg:
            print(msg)
            if mode_aux == 'w':
                DFrame.to_sql(tb_name, cnx, if_exists='replace')
            else:
                DFrame.to_sql(tb_name, cnx, if_exists='append')
    os.chdir(dir_1)
    print('Se han añadido los registros a la tabla',tb_name)
    
            
def Rexcel_wpd(path,nfile,sheet_name):
    try:
        a=pd.read_excel(path+os.sep+nfile,sheet_name,enconding='latin_1')
    except Exception as msg:
        print('NO se pudo aplicar la codificacion latin_1')
        print(msg)
        a=pd.read_excel(path+os.sep+nfile,sheet_name)
    return(a)
    
def make_Siebel_Qrylist(cad):
    chain=cad
    chain=chain.strip('\n').replace('\n',' or ').upper()
    return(chain)
    
def accent_clean(cad):
    try:
        return(str(cad).replace('Á','A').replace('É','E').replace('Í','I').replace('Ó','O').replace('Ú','U').replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u'))
    except Exception as msg:
        print(msg)
        return(str(cad))
    
def top10_oracle_table(cve,tb_name):
    qry_1='''select * from '''
    cond=''' where rownum<=10'''
    qry=qry_1+tb_name+cond
    try:
        return(run_qry_pd_default(cve,qry))
    except Exception as msg:
        print(msg)
        return(False)
        
def string_to_date(chain):
    try:
        return(parser.parse(str(chain)))
    except Exception as msg:
        print(msg)
        return(chain)
    
    
def WExcel_Wpd(DFrame_list,Names_DFrame_list,Path,FName):
    band=0
    writer=pd.ExcelWriter(Path+os.sep+FName+'.xlsx',engine='xlsxwriter')
    for i in DFrame_list:
        i.to_excel(writer,sheet_name=Names_DFrame_list[band],index=False)
        band=band+1
    writer.save()
    
