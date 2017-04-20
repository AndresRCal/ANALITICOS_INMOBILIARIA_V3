# -*- coding: utf-8 -*-
"""
Created on Wed Oct 19 18:57:18 2016

@author: AAREYESC
"""

import os
import csv
import pandas as pd
import cx_Oracle

os.environ['NLS_LANG'] ='.UTF8'

def prueba_conexion():
    try:
        cx_Oracle.connect('USR_INMERCADOS','Liverpool02','crmpdb-scan.puerto.liverpool.com.mx:1527/CRMP')#CRM_PRODUCCION
        return(1)
    except Exception as rep:
        return(0)

def ContarCommasGalerias(x):
    ContadorComma=0
    ContadorEsp=0
    ContadorNs=0
    for letra in x:
        if letra == ',':
            ContadorComma=ContadorComma + 1
        if letra == ' ':
            ContadorEsp=ContadorEsp + 1
        if letra == '\n':
            ContadorNs=ContadorNs +1
    return(ContadorComma,ContadorEsp,ContadorNs)

def desmenuzar_brief(Work_Root,Cataloges_Root):
    print('Desmenuzando los briefs...')
    os.chdir(Cataloges_Root)
    if os.path.isfile('cc_catalog.csv') == True:
        dic_dev_levfile=pd.DataFrame(pd.read_csv('cc_catalog.csv',sep=','))
        Dict_Mec_Prom_Prod=pd.DataFrame(pd.read_csv('catalogo_promomprod.csv',sep=','))
        dict_tempo_cat=pd.DataFrame(pd.read_csv('tempo_catalog.csv',sep=','))
    else:
        print('LOS ARCHIVOS DE CATALOGOS NO EXISTEN!!!!!')
    
    os.chdir(Work_Root)
    if os.path.isfile('Briefs_sucios.csv') == True:
        c=csv.DictReader(open('Briefs_sucios.csv','r'))
    ##############################TRABAJAR SOBRE GALERIAS########################
        print('Parseo de Galerias...')
        b_id=''
        interm=''
        new_array1=[]
        new_array2=[]
        for i in c:
            b_id=i['BRIEF_ID']
            ContadorComma,ContadorEsp,ContadorNs=ContarCommasGalerias(i['GALERIAS'].strip())
            #print(i['GALERIAS'].strip())
            if ContadorComma > 0:
                interm=i['GALERIAS'].strip().replace('\n','').replace('\t','').replace('\r','').replace('GALERIAS','').replace('G.','').replace(' G ','').replace(' Y ',',').split(',')
            elif ContadorEsp > 2 and i['GALERIAS'].strip() != 'SAN JUAN DEL RIO':
                interm=i['GALERIAS'].strip().replace('\n','').replace('\t','').replace('\r','').replace('GALERIAS','').replace('G.','').replace(' G ','').replace(' Y ',' ').split(' ')
            elif ContadorNs > 1:
                interm=i['GALERIAS'].strip('\n').strip().replace('\t','').replace('\r','').replace('GALERIAS','').replace('G.','').replace(' G ','').replace(' Y ','\n').replace('\n',',').strip(',').replace(", '',",",").replace(",'',",",").replace(''',,''',',').split(',')
                #print('esto es lo mando',interm)
            else:
                interm=i['GALERIAS'].strip().replace('\n','').replace('\t','').replace('\r','').replace('GALERIAS','').replace('G.','').replace(' G ','').replace(' Y ',',').split(',')
            band=0
            for j in interm:
                new_array1+=[b_id]
                #print (str(interm[band]).strip())
                new_array2+=[str(interm[band]).strip()]
                band=band+1
        dict_ind_gal={
        'BRIEF_ID': pd.Series(new_array1),'CC':pd.Series(new_array2)
        }
        try:
            ind_gal=pd.DataFrame(dict_ind_gal)
        except Exception as msg:
            print(msg)
            
        
     
    ##############################TRABAJAR SOBRE Las mecanicas de la promocion#####
        print('Interpretacion tipo de mecanica')
        c=csv.DictReader(open('Briefs_sucios.csv','r'))
        new_array1=[]
        new_array2=[]
        
        for i in c:
            new_array1+=[i['BRIEF_ID']]
            new_array2+=i['TIPODEMECANICA'].replace('\n','').replace('\t','').replace('\r','').split(',')
            
        Dict_BrMec_Prom_Prod={
                              'BRIEF_ID':pd.Series(new_array1),'TIPODEMECANICA':pd.Series(new_array2)
                              }
        BrMec_Prom_Prod=pd.DataFrame(Dict_BrMec_Prom_Prod)
    ##############################TRABAJAR SOBRE LAS TEMPORADAS#####
        print('Interpretacion de la temporada de los Briefs..')                          
        c=csv.DictReader(open('Briefs_sucios.csv','r'))
        b_id=''
        interm=''
        new_array1=[]
        anio=[]
        temp_br=[]
        for i in c:
            b_id=i['BRIEF_ID']
            interm=i['NOMBRE_BRIEF'].replace('\n','').replace('\t','').replace('\r','').split('-')
            band=1
            for j in interm:
                if band == 1:
                    new_array1+=[b_id]
                    anio+=[interm[0].replace('I','')]
                    temp_br+=[interm[2].strip(' ')]
                    band=band+1
                else:
                    break
            
            dict_temp_anio_bono={
                                 'BRIEF_ID':pd.Series(new_array1),'ANIO':pd.Series(anio),'key':pd.Series(temp_br)
                                 }
            temp_anio_bono=pd.DataFrame(dict_temp_anio_bono,columns=['BRIEF_ID','ANIO','key'])       
    
    ##############################TRABAJAR MERGEs#####
    merg1=pd.merge(ind_gal,BrMec_Prom_Prod,on='BRIEF_ID',how='left')
    merg2=pd.merge(merg1,temp_anio_bono,on='BRIEF_ID',how='left')
    merg3=pd.merge(merg2,dic_dev_levfile,on='CC',how='left')
    merg4=pd.merge(merg3,dict_tempo_cat,left_on='key',right_on='NUMERO',how='left')
    merg5=pd.merge(merg4,Dict_Mec_Prom_Prod,left_on='TIPODEMECANICA',right_on='MECANICA PROMOCIONES',how='left')
    merg5.to_csv('Brief_desmenuzado.csv',index=False,encoding='latin-1')
    return()
if __name__ == "__main__":
    desmenuzar_brief(
                     Work_Root=os.getcwd()+os.sep,
                     Cataloges_Root=os.getcwd()+os.sep+"Catalogos_base"
                     )

    
def Consecutivo_global(x):
    try:
        db=cx_Oracle.connect('USR_INMERCADOS','Liverpool02','crmpdb-scan.puerto.liverpool.com.mx:1527/CRMP')
        c=db.cursor()
        cns_qry1='''
    select  NVL(Global_CNS,0) from
        (
        SELECT MAX(cast(SUBSTR(NAME,(INSTR(NAME,'-',-1)+1),cast(length(NAME) as numeric(20,0))-cast(INSTR(NAME,'-',-1) as numeric(20,0))) as integer)) as Global_CNS
        FROM SIEBEL.S_LOY_PROMO
        where LOY_PROG_ID = '1-6VA2A1' AND NAME LIKE 'I%' and SUBSTR(NAME,-1,1) in ('1','2','3','4','5','6','7','8','9','0')    
    '''
        t_qry=cns_qry1+" and name like '"+x+"%'"+')'
        c.execute(t_qry)
        for cx in c:
            return(cx[0])
        c.close()
    except Exception as msg:
        print(msg)
        return(0)

#Work_Root=('D:\\soporte inmobi\\Promos\\Pase_a_produc\\B5_2016\\10_31_16_B5_2016.V0\\')
#calculo de Nombre promocion y productos Mecanica con consecutivo (sincronizado con QA)

def Calculo_NPromoProd(Work_Root):
    print('Iniciando el calculo de los nombres y consecutivos...')
    os.chdir(Work_Root)
    s=csv.DictReader(open('Brief_desmenuzado.csv','r'))
    NPromocion=[]
    NPMecanica=[]
    NPromocion_cns=[]
    NPMecanica_cns=[]
    BRIEF_ID_NPromo=[]
    CATEGORIA_DE_PRODUCTO=[]
    cns=1
    lst_cns=[]
    ABREVIATURA=[]
    band_conn=prueba_conexion()
    if band_conn == 0:
        print('Prueba de Conexion fallida se procedera con calculo de consecutivos local solamente...')
    for i in s:
        cnst=0
        if i == None:
            continue
#        if  i['BRIEF_ID'] in BRIEF_ID_NPromo and 'I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-' in NPromocion:
#            cns=cns-1
        if len(NPromocion) > 0:
            if 'I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-' in NPromocion:
                cns=cns+1
            else:
                cns=1
        else:
            cns=1
        BRIEF_ID_NPromo+=[i['BRIEF_ID']]
        NPromocion+=['I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-']
        print('nombre promocion: ','I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-')
        print('consecutivo 1: ',cns)
        NPMecanica+=[i['ABREVIATURA']+'-'+i['ANIO'].replace('.0','')+i['ABREVIACION']+'-'+i['MECANICA PRODUCTOS MECANICA']+'-']
        #Punto de mejora (consultar una sola vez a la BD por todos los iguales)
        if i['ABREVIATURA'] is None or i['ABREVIACION'] is None or i['MECANICA PROMOCIONES'] is None:
            cns2=0
        elif band_conn == 0:
            cns2=0
        else:
            cns2=Consecutivo_global('I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-')
        cnst=cns+cns2
        print('consecutivo 1:',cns)
        print('consecutivo 2:',cns2)
        print(cnst)
        lst_cns+=[cnst]
        NPromocion_cns+=['I'+i['ANIO'].replace('.0','')+'-'+i['ABREVIATURA']+'-'+i['ABREVIACION']+'-'+i['MECANICA PROMOCIONES']+'-'+str(cnst)]
        NPMecanica_cns+=[i['ABREVIATURA']+'-'+i['ANIO'].replace('.0','')+i['ABREVIACION']+'-'+i['MECANICA PRODUCTOS MECANICA']+'-'+str(cnst)]
        ABREVIATURA+=[i['ABREVIATURA']]
        CATEGORIA_DE_PRODUCTO+=[i['CATEGORIA DE PRODUCTO']]    
        
    dict_Nomb_PromProdM={
          'BRIEF_ID':pd.Series(BRIEF_ID_NPromo),
          'NPromocion_cns':pd.Series(NPromocion_cns),
          'NPMecanica_cns':pd.Series(NPMecanica_cns),
          'ABREVIATURA':pd.Series(ABREVIATURA),
          'CATEGORIA DE PRODUCTO':pd.Series(CATEGORIA_DE_PRODUCTO)
          }
    Nomb_PromProdM1=pd.DataFrame(dict_Nomb_PromProdM)
    try:
        df_Brief_desmenuzado=pd.DataFrame(pd.read_csv('Brief_desmenuzado.csv',sep=','))
    except Exception as msg:
        print(msg)
        df_Brief_desmenuzado=pd.DataFrame(pd.read_csv('Brief_desmenuzado.csv',sep=',',encoding='latin-1'))
    merg1=pd.merge(Nomb_PromProdM1,df_Brief_desmenuzado,on=['BRIEF_ID','ABREVIATURA','CATEGORIA DE PRODUCTO'],how='left')
    merg1.to_csv('T_Stage1.csv',index=False,encoding='latin-1')
    print('Se ha finalizado el calculo de los nombres y consecutivos')
    return()
if __name__ == "__main__":
    Calculo_NPromoProd(Work_Root=os.getcwd()+os.sep)