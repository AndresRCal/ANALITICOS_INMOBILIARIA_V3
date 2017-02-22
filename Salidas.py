# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 17:08:11 2016

@author: AAREYESC
"""

import os
import csv
import pandas as pd
import desmenuzar_briefs_V2 as dzar
os.environ['NLS_LANG'] ='.UTF8'

def confirmacionarchs_min(Work_Root):
    os.chdir(Work_Root)
    if os.path.isfile('T_Stage1.csv') == False:
        dzar.Calculo_NPromoProd(Work_Root)
if __name__ == "__main__":
    confirmacionarchs_min(Work_Root=os.getcwd()+os.sep)
        
def analiticos_promocionInmo(Work_Root):
    os.chdir(Work_Root)
    try:
        DF_Briefs=pd.DataFrame(pd.read_csv('Briefs_sucios.csv',sep=',',encoding='latin-1'))
    except Exception as msg:
        print(msg)
        DF_Briefs=pd.DataFrame(pd.read_csv('Briefs_sucios.csv',sep=',',encoding='UTF8'))
    DF_Briefs2=DF_Briefs[['BRIEF_ID','BENEFICIO','PROVEEDOR','FECHAINICIAL','FECHASFINAL']]
    try:
        DF_BCrit=pd.DataFrame(pd.read_csv('BASE_CRITERIOS.csv',sep=',',encoding='latin-1'))
    except Exception as msg:
        print(msg)
        DF_BCrit=pd.DataFrame(pd.read_csv('BASE_CRITERIOS.csv',sep=',',encoding='UTF8'))
    DF_BCrit_2=DF_BCrit[['BRIEF_ID','NOMBRE_BRIEF','TROFEO','TICKET_MAXIMOS','MONTO_MINIMO','CATEGORIA_TIENDAS','Participacion_RS','Redenciones_Maximas','CANTIDAD_BENEFICIOS']]
    try:
        STG_1=pd.DataFrame(pd.read_csv('T_Stage1.csv',sep=',',encoding='UTF8'))
    except Exception as msg:
        print(msg)
        STG_1=pd.DataFrame(pd.read_csv('T_Stage1.csv',sep=',',encoding='latin-1'))        
    STG_2=STG_1[['BRIEF_ID','NPMecanica_cns','NPromocion_cns','ABREVIATURA','CC','Nombre Galeria','MECANICA PROMOCIONES','MECANICA PRODUCTOS MECANICA','TEMPORADA','Prod_Meca_Brinda_Bono','CATEGORIA DE PRODUCTO']]
    MERG1=pd.merge(STG_2,DF_BCrit_2,on='BRIEF_ID',how='left')
    MERG2=pd.merge(MERG1,DF_Briefs2,on='BRIEF_ID',how='left')
    MERG2.to_csv('T_Stage2.csv',index=False,encoding='latin-1')
    
    c1=csv.DictReader(open('T_Stage2.csv','r'))
    NPMecanica_cns=[]
    NPromocion_cns=[]
    BRIEF_ID=[]
    P_Bono=[]
    PROMPROD_DESC=[]
    for i in c1:
          crit_trofeo='',
          crit_cat_tienda=''
          crit_monto_min=''
          crit_Part_RS=''
          crit_reden_max=''
          crit_ticket_max=''
          crit_galeria=''
          BRIEF_ID+=[i['BRIEF_ID']]
          NPMecanica_cns+=[i['NPMecanica_cns']]
          NPromocion_cns+=[i['NPromocion_cns']]
          P_Bono+=[i['PROVEEDOR'].replace('\t','').replace('\n','').replace('\r','')+'-'+i['BENEFICIO'].replace('\t','').replace('\n','').replace('\r','')]
          if i['TROFEO'] != '':
                crit_trofeo='TROFEO:'+i['TROFEO'] +','
          else:
                crit_trofeo=''
          if i['ABREVIATURA'] != '':
                crit_galeria='GALERIA:'+i['ABREVIATURA'] +','
          else:
                crit_galeria=''
          if i['CATEGORIA_TIENDAS'] != '':
                crit_cat_tienda='CATEGORIA DE TIENDAS:'+i['CATEGORIA_TIENDAS'] +','
          else:
                crit_cat_tienda=''
          if i['Redenciones_Maximas'] != '':
                crit_reden_max='Redenciones Maximas:'+i['Redenciones_Maximas']
          else:
                crit_reden_max=''
          if i['Participacion_RS'] != '':
                crit_Part_RS='Participacion_RS:'+i['Participacion_RS'] +','
          else:
                crit_Part_RS=''
          if i['MONTO_MINIMO'] != '':
                crit_monto_min='MONTO MINIMO: '+'$'+i['MONTO_MINIMO'].replace('.00','') +','
          else:
                crit_monto_min=''
          if i['TICKET_MAXIMOS'] != '':
                crit_ticket_max='TICKET MAXIMOS:'+i['TICKET_MAXIMOS'].replace('.0','') +','
          else:
                crit_ticket_max=''
          PROMPROD_DESC+=[i['PROVEEDOR'].replace('\t','').replace('\n','').replace('\r','')+'-'+i['BENEFICIO'].replace('\t','').replace('\n','').replace('\r','')+':  '+crit_galeria+crit_trofeo+crit_ticket_max+crit_monto_min+crit_cat_tienda+crit_Part_RS+crit_reden_max]
          
    dict_archivo_relacion={
          'BRIEF_ID':pd.Series(BRIEF_ID),
          'NPromocion_cns':pd.Series(NPromocion_cns),
          'NPMecanica_cns':pd.Series(NPMecanica_cns),
          'DESCRIPCION':pd.Series(PROMPROD_DESC)
          }
    
    df_relacion=pd.DataFrame(dict_archivo_relacion)
    MERG3=pd.merge(MERG2,df_relacion,on=['BRIEF_ID','NPromocion_cns','NPMecanica_cns'],how='left')
    MERG3[['BRIEF_ID','NOMBRE_BRIEF','NPromocion_cns','FECHAINICIAL','FECHASFINAL','NPMecanica_cns','Nombre Galeria','Prod_Meca_Brinda_Bono',
           'TROFEO','TICKET_MAXIMOS', 'MONTO_MINIMO', 'CATEGORIA_TIENDAS','Participacion_RS', 'Redenciones_Maximas', 'CANTIDAD_BENEFICIOS',
           'DESCRIPCION','CATEGORIA DE PRODUCTO']]
    df_bono=pd.DataFrame(pd.read_csv('Productos_Bono.csv',sep=',',encoding='latin-1'))       
    df_bono=df_bono[['BRIEF_ID','NOMBRE_PRODUCTO']]    
    MERG4=pd.merge(MERG3,df_bono,on='BRIEF_ID',how='left')
    MERG4[['BRIEF_ID','NOMBRE_BRIEF','NPromocion_cns','FECHAINICIAL','FECHASFINAL','NPMecanica_cns','Nombre Galeria','Prod_Meca_Brinda_Bono',
           'NOMBRE_PRODUCTO','TROFEO','TICKET_MAXIMOS', 'MONTO_MINIMO', 'CATEGORIA_TIENDAS','Participacion_RS', 'Redenciones_Maximas',
           'CANTIDAD_BENEFICIOS','DESCRIPCION','CATEGORIA DE PRODUCTO']].to_csv('T_Stage2.csv',index=False,encoding='latin-1')
if __name__ == "__main__":
    analiticos_promocionInmo(Work_Root=os.getcwd()+os.sep)
    
def analiticosproductos_fidelizacion(Work_Root):
    c1=csv.DictReader(open('T_Stage2.csv','r'))
    BRIEF_ID=[]
    ESTADO=[]
    NOMBRE_PRODUCTO=[]
    DESCRIPCION=[]
    NO_BENEFICIOS_INICIALES=[]
    BENEFICIOS_EXISTENTES=[]
    CATEGORIA_PRODUCTO=[]
    FECHA_INICIO_VIGENCIA=[]
    FECHA_FINAL_VIGENCIA=[]
    TIPO=[]
    PROVEEDOR=[]
    CATEGORIA=[]
    TIPO_PRODUCTO=[]
    LINEA_DE_PRODUCTO=[]
    DISPONIBLE_PARA_PEDIDO=[]
    for i in c1:
        BRIEF_ID+=[i['BRIEF_ID']]
        ESTADO+=['Activo']
        NOMBRE_PRODUCTO+=[i['NPMecanica_cns']]
        DESCRIPCION+=[i['DESCRIPCION']]
        NO_BENEFICIOS_INICIALES+=[i['CANTIDAD_BENEFICIOS']]
        BENEFICIOS_EXISTENTES+=[i['CANTIDAD_BENEFICIOS']]
        CATEGORIA_PRODUCTO+=[i['CATEGORIA DE PRODUCTO']]
        FECHA_INICIO_VIGENCIA+=[i['FECHAINICIAL']]
        FECHA_FINAL_VIGENCIA+=[i['FECHASFINAL']]
        TIPO+=['Producto']
        PROVEEDOR+=['']
        CATEGORIA+=['']
        TIPO_PRODUCTO+=['']
        LINEA_DE_PRODUCTO+=['INMOBILIARIA']
        DISPONIBLE_PARA_PEDIDO+=['Y']
    dict_prod_meca={
          'BRIEF_ID':pd.Series(BRIEF_ID),
          'ESTADO':pd.Series(ESTADO),
          'NOMBRE_PRODUCTO':pd.Series(NOMBRE_PRODUCTO),
          'DESCRIPCION':pd.Series(DESCRIPCION),
          'NO_BENEFICIOS_INICIALES':pd.Series(NO_BENEFICIOS_INICIALES),
          'BENEFICIOS_EXISTENTES':pd.Series(BENEFICIOS_EXISTENTES),
          'CATEGORIA_PRODUCTO':pd.Series(CATEGORIA_PRODUCTO),
          'FECHA_INICIO_VIGENCIA':pd.Series(FECHA_INICIO_VIGENCIA),
          'FECHA_FINAL_VIGENCIA':pd.Series(FECHA_FINAL_VIGENCIA),
          'TIPO':pd.Series(TIPO),
          'PROVEEDOR':pd.Series(PROVEEDOR),
          'CATEGORIA':pd.Series(CATEGORIA),
          'TIPO_PRODUCTO':pd.Series(TIPO_PRODUCTO),
          'LINEA_DE_PRODUCTO':pd.Series(LINEA_DE_PRODUCTO),
          'DISPONIBLE_PARA_PEDIDO':pd.Series(DISPONIBLE_PARA_PEDIDO)
          }
    df_prod_meca=pd.DataFrame(dict_prod_meca)
    df_prod_bono=pd.DataFrame(pd.read_csv('Productos_Bono.csv',sep=',',encoding='latin-1'))
    T_prods=pd.concat([df_prod_bono,df_prod_meca]).drop_duplicates()
    T_prods.fillna('')
    T_prods[['BRIEF_ID','ESTADO','NOMBRE_PRODUCTO','DESCRIPCION','NO_BENEFICIOS_INICIALES','BENEFICIOS_EXISTENTES',
             'CATEGORIA_PRODUCTO','FECHA_INICIO_VIGENCIA','FECHA_FINAL_VIGENCIA','TIPO','PROVEEDOR',
             'CATEGORIA','TIPO_PRODUCTO','LINEA_DE_PRODUCTO','DISPONIBLE_PARA_PEDIDO']].to_csv('T_prods.csv',index=False,encoding='latin-1')
if __name__ == "__main__":
    analiticosproductos_fidelizacion(Work_Root=os.getcwd()+os.sep)