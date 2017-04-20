# -*- coding: utf-8 -*-
"""
Created on Tue Mar 14 10:26:39 2017

@author: AAREYESC
"""
import FandVars.alGusto as alg
import os

dir_auxiliar='''C:\\Users\\AAREYESC\\Documents\\GitHub\\ANALITICOS_INMOBILIARIA_V3'''
config_promo_report_qry='''
Select
qry_base.PROGRAMA_ID as "ID DE PROGRAMA",
qry_base.promo_id AS "ID DE PROMOCION",
prod_meca_id AS "ID PROD MECANICA",
bono_id AS "ID BONO",
YA_GANO AS "YA GANO (REDENCIONES)",
MAX_TICKETS AS "MAX TICKETS",
MONTO_VALIDO AS "MONTO VALIDO",
TICKET_VALIDO AS "TICKET VALIDO",
MONTO_ACUMULADO AS "MONTO ACUMULADO",
NO_DE_TICKETS AS "No DE TICKETS",
REDENCIONES AS "REDENCIONES",
YA_PARTICIPO AS "YA PARTICIPO",
CUPONES_GANADOS AS "CUPONES GANADOS",
ACTIVA AS "ACITVA",
promo_name AS "NOMBRE DE PROMOCION",
promo_fecha_ini AS "INICIO VIGENCIA PROMO",
promo_fecha_fin AS "FIN VIGENCIA PROMO",
CC AS "CENTRO COMERCIAL",
prod_meca_name AS "NOMBRE PROD MECANICA",
qry_base.Mecanica_Producto AS "MECANICA DE PRODUCTO",
prod_meca_benef_ini AS "BENEFICIOS INICIALES",
prod_meca_benef_exist AS "BENEFICIOS EXISTENTES",
prod_bono_name AS "NOMBRE BONO",
descripcion AS "DESCRIPCION"
from 
(
		select 
		case when promo.PROGRAMA_ID = '1-6VA2A1' then 'Inmobiliaria' else promo.PROGRAMA_ID end as PROGRAMA_ID,
		promo.PROMO_ID as promo_id,
		meca.PROD_MEC_ID as prod_meca_id,
		bono.ID_BONO as bono_id,
		promo.ACTIVA as ACTIVA,
		promo.PROMO_NAME as promo_name, 
		promo.INI_VIG_PROMO as promo_fecha_ini, 
		promo.FIN_VIG_PROMO as promo_fecha_fin, 
		meca.PROD_MEC_NAME as prod_meca_name,
    meca.Mecanica_Producto as Mecanica_Producto,
		meca.Benef_Init as prod_meca_benef_ini,
		meca.Benef_exist as prod_meca_benef_exist,
		meca.descripcion as descripcion,
		bono.PROD_BONO_NAME as prod_bono_name
		from
			(
				SELECT 
				ROW_ID AS PROMO_ID, 
				NAME AS PROMO_NAME, 
				LOY_PROG_ID AS PROGRAMA_ID, 
				ACTIVE_FLG AS ACTIVA, 
				TO_CHAR(START_DT,'YYYY/MM/DD') AS INI_VIG_PROMO, 
				TO_CHAR(END_DT,'YYYY/MM/DD') AS FIN_VIG_PROMO 
				FROM SIEBEL.S_LOY_PROMO where LOY_PROG_ID ='1-6VA2A1'
			) promo left join
			(
				SELECT 
				TPM.ROW_ID AS PROMO_ID, 
				TPD.ROW_ID AS PROD_MEC_ID, 
				TPD.NAME AS PROD_MEC_NAME,
		    TPD.UNITS_BCKORD as Benef_Init,
		    TPD.UNITS_INVENT as Benef_exist,
        TPD.CATEGORY_CD AS Mecanica_Producto,
		    TPD.DESC_TEXT as descripcion
				FROM SIEBEL.S_LOY_PROMO TPM, SIEBEL.S_LOY_PROMOPRD TR, SIEBEL.S_PROD_INT TPD 
				WHERE TPM.ROW_ID=TR.PROMO_ID AND TR.PRODUCT_ID=TPD.ROW_ID
			) meca on promo.PROMO_ID = meca.PROMO_ID left join
			(
				SELECT 
				TPM.ROW_ID AS PROMO_ID, 
				TAC.PROD_ID AS ID_BONO,
				TPD.NAME AS PROD_BONO_NAME
				FROM SIEBEL.S_LOY_PROMO TPM, SIEBEL.S_LOY_PROMO_RL TRL ,SIEBEL.S_LOY_ACTCRTR TAC, SIEBEL.S_PROD_INT TPD
				WHERE  TAC.PROD_ID IS NOT NULL AND TRL.ROW_ID=TAC.PROMO_RULE_ID AND TPM.ROW_ID=TRL.PROMO_ID AND TPD.ROW_ID=TAC.PROD_ID
			) bono on promo.PROMO_ID = bono.PROMO_ID 
		--order by PROGRAMA_ID,promo.PROMO_NAME asc
) qry_base left join 
(
		select PROMO_ID,
		LISTAGG(CC, ',') WITHIN GROUP (ORDER BY CC) AS CC,
		LISTAGG(YA_GANO, ',') WITHIN GROUP (ORDER BY YA_GANO) AS YA_GANO,
		LISTAGG(MAX_TICKETS, ',') WITHIN GROUP (ORDER BY MAX_TICKETS) AS MAX_TICKETS,
		LISTAGG(MONTO_VALIDO, ',') WITHIN GROUP (ORDER BY MONTO_VALIDO) AS MONTO_VALIDO,
		LISTAGG(TICKET_VALIDO, ',') WITHIN GROUP (ORDER BY TICKET_VALIDO) AS TICKET_VALIDO,
		LISTAGG(MONTO_ACUMULADO, ',') WITHIN GROUP (ORDER BY MONTO_ACUMULADO) AS MONTO_ACUMULADO,
		LISTAGG(NO_DE_TICKETS, ',') WITHIN GROUP (ORDER BY NO_DE_TICKETS) AS NO_DE_TICKETS,
		LISTAGG(REDENCIONES, ',') WITHIN GROUP (ORDER BY REDENCIONES) AS REDENCIONES,
		LISTAGG(YA_PARTICIPO, ',') WITHIN GROUP (ORDER BY YA_PARTICIPO) AS YA_PARTICIPO,
		LISTAGG(CUPONES_GANADOS, ',') WITHIN GROUP (ORDER BY CUPONES_GANADOS) AS CUPONES_GANADOS
		from
		(    
		    Select
		    promo.PROMO_ID,
		    case when upper(ATRIB_NAME) = 'CC' then ATRIB_VAL else null end as "CC",
		    case when upper(ATRIB_NAME) = 'YA GANO' then ATRIB_VAL else null end as "YA_GANO",
		    case when upper(ATRIB_NAME) = 'MAX TICKETS' then ATRIB_VAL else null end as "MAX_TICKETS",
		    case when upper(ATRIB_NAME) = 'MONTO VALIDO' then ATRIB_VAL else null end as "MONTO_VALIDO",
		    case when upper(ATRIB_NAME) = 'TICKET VALIDO' then ATRIB_VAL else null end as "TICKET_VALIDO",
		    case when upper(ATRIB_NAME) = 'MONTO ACUMULADO' then ATRIB_VAL else null end as "MONTO_ACUMULADO",
		    case when upper(ATRIB_NAME) = 'NO DE TICKETS' then ATRIB_VAL else null end as "NO_DE_TICKETS",
		    case when upper(ATRIB_NAME) = 'REDENCIONES' then ATRIB_VAL else null end as "REDENCIONES",
		    case when upper(ATRIB_NAME) = 'YA PARTICIPO' then ATRIB_VAL else null end as "YA_PARTICIPO",
		    case when upper(ATRIB_NAME) = 'CUPONES GANADOS' then ATRIB_VAL else null end as "CUPONES_GANADOS"
		    from 
		    (
		        SELECT 
		        ROW_ID AS PROMO_ID, 
		        LOY_PROG_ID AS PROGRAMA_ID
		        FROM SIEBEL.S_LOY_PROMO where LOY_PROG_ID = '1-6VA2A1' --and name like 'I17%'
		      ) promo left join
		    (
		        SELECT 
		        DISPLAY_NAME AS ATRIB_NAME,
		        DEFAULT_VAL AS ATRIB_VAL,
		        PROMOTION_ID AS PROMO_ID,
		        PROGRAM_ID AS PROGRAM_ID
		        FROM SIEBEL.S_LOY_ATTRDEFN
		        where OBJECT_CD = 'Promoción' and PROGRAM_ID = '1-6VA2A1'
		    ) atrib on promo.PROMO_ID = atrib.PROMO_ID and promo.PROGRAMA_ID = atrib.PROGRAM_ID
		) u group by PROMO_ID
) qry_atribs on qry_base.promo_id=qry_atribs.PROMO_ID
'''

def post_config_promo_report(cond=''):
    if cond != '':
        cond=''' where '''+str(cond)+''' order by CC,promo_name asc, prod_meca_name desc'''
    else:
        cond=' order by CC,promo_name asc, prod_meca_name desc'
    qry_to_run=str(config_promo_report_qry)+str(cond)
#    print(qry_to_run)
    try:
        ltyprod=alg.run_qry_pd_default('PROD LTY',qry_to_run)
        return(ltyprod)
    except Exception as msg:
        print(msg)
        return(False)

def report_delimitation(Work_Root=os.getcwd()):
    df=alg.Rcsv_Wpandas('T_Stage2.csv',Work_Root)
    lst_names=[]
    lst_names=list(df['NPromocion_cns'])
    str_name='\n'
    for i in lst_names:
        str_name+=str(i)
        str_name+='\n'
    return('promo_name in ('+alg.MakinSQLconditionList(str_name)+')')

def Making_the_report(Work_Root=os.getcwd(),temporada='pba'):
    if os.path.exists(Work_Root+os.sep+'T_Stage2.csv') == True and  os.path.exists(Work_Root+os.sep+'Briefs_sucios.csv') == True:
        cond=report_delimitation(Work_Root)
        ext_prod=post_config_promo_report(cond)
        analiticos=alg.Rcsv_Wpandas('T_Stage2.csv',Work_Root)
        os.chdir(dir_auxiliar)
        briefs=alg.Rcsv_Wpandas('Briefs_sucios.csv',Work_Root)
        DF_lst=[briefs,analiticos,ext_prod]
        df_lst_names=['briefs','analiticos','ext_prod']
        if os.path.exists(Work_Root+os.sep+alg.dar_nombre(temporada,'PROMOCIONES_Inmo','xlsx')):
            os.remove(Work_Root+os.sep+alg.dar_nombre(temporada,'PROMOCIONES_Inmo','xlsx'))
        else:
            pass
        alg.WExcel_Wpd(DF_lst,df_lst_names,Work_Root,alg.dar_nombre(temporada,'PROMOCIONES_Inmo','xlsx'))
        return(True)
    else:
        print('Favor de validar la existencia de los archivos T_Stage2 y Briefs_sucios')
        return(False)
    
temp=input('''Nombre de la temporada del reporte''')
Making_the_report(os.getcwd(),temp)
print('Muchas gracias, Saludos!')