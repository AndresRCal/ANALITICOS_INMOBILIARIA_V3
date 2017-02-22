# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 21:46:17 2016

@author: AAREYESC
"""
import os
import cx_Oracle
import pandas as pd

os.environ['NLS_LANG'] ='.UTF8'

def queries_base(Work_Root):
    
    os.chdir(Work_Root)  
    
    #---------------------------Dar de alta conexiones
    db=cx_Oracle.connect('USR_INMERCADOS','Liverpool02','crmpdb-scan.puerto.liverpool.com.mx:1527/CRMP')#CRM_PRODUCCION
#    db=cx_Oracle.connect('USR_TOKEN','UsR_l1VT0K1982','172.16.203.135:1527/CRMQ')#QA
    c=db.cursor()
    
    
    qry_br_extractor='''
                SELECT  
                ROW_ID as Brief_id,
                UPPER(TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as NOMBRE_BRIEF,
                UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) AS MECANICA_DE_LA_PROMOCION,
                UPPER(TRANSLATE(GALLERY,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as Galerias,
                START_DATE as FechaInicial,
                END_DATE as FechasFinal,
                UPPER(TRANSLATE(PROVIDER,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as Proveedor,
                UPPER(TRANSLATE(BENEFIT,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as Beneficio,
                BENEFIT_NUM as NumDeBeneficios,
                UPPER(TRANSLATE(BENEFIT_DESC,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as DescripcionDelBeneficio,
                UPPER(PROVIDER_EXPERTISE) as Categoria,
                UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as Restricciones,
                UPPER(PRODUCT_CATEGORY) as TipoDeMecanica,
                COUPON_NUM as Cantidad_Cupones,
                UPPER(TRANSLATE(COUPON_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) as Restriccion_Cupones,
                UPPER(nvl(IS_EXTERNAL,'F')) as Marca_externa 
                FROM SIEBEL.CX_BRIEF_REQ 
    '''
    sb_qry_Bonos_1='''
                SELECT 
                Brief_id,
                NAME as BRIEF_NAME,
                'Activo' AS Estado,
                case when INSTR(Prod_BONO,'-',1) = 1 then TRIM(SUBSTR(Prod_BONO,2,length(Prod_BONO))) else Prod_BONO end as Nombre_Producto,
                '' as Descripcion,
                '' as Beneficios_Existentes,
                '' as No_de_Beneficios_Iniciales,
                '' as Categoría_de_producto,
                '' as Fecha_de_inicio_de_vigencia,
                '' as Fecha_final_de_vigencia,
                'INMOBILIARIA' as Linea_de_productos,
                TIPO,
                Proveedor,
                'I-BNO-'||DECODE(CATEGORIA_PRODUCTO,'ESTRENOS','SOLO ESTRENOS',TRANSLATE(CATEGORIA_PRODUCTO,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')) AS Categoría,
                TIPO_PRODUCTO,
                Marca_externa
                from
                (
                select
                ROW_ID as Brief_id,
                TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') as NAME,
                TRANSLATE(PROVIDER,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') as Proveedor,
                case when INSTR(PRODUCT_CATEGORY,'TROFEO') = 0 then trim(SUBSTR(TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),INSTR(NAME,'-',INSTR(NAME,'-',7,1)+1,1)+1,length(NAME)))
                     when INSTR(NAME,'TROFEO ') != 0 then trim(SUBSTR(TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),(INSTR(NAME,'TROFEO')+length('TROFEO  X')),length(NAME))) 
                     when INSTR(NAME,'TROFEO') != 0 then trim(SUBSTR(TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),(INSTR(NAME,'TROFEO')+length('TROFEO  ')),length(NAME))) 
                     else null end as Prod_BONO,
                CASE WHEN INSTR(PRODUCT_CATEGORY,'TROFEO') = 0 then 'SIN CATEGORIA'
                     WHEN INSTR(upper(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'DE LA CATEGORIA DE')       != 0 THEN TRIM(SUBSTR(DESCRIPTION,INSTR(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),'DE LA CATEGORIA DE')+LENGTH('DE LA CATEGORIA DE'),LENGTH(DESCRIPTION)))
                     WHEN INSTR(upper(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'DE LA CATEGORIA') != 0 THEN TRIM(SUBSTR(DESCRIPTION,INSTR(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),'DE LA CATEGORIA')+LENGTH('DE LA CATEGORIA'),LENGTH(DESCRIPTION))) 
                     WHEN INSTR(upper(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'CATEGORIA DE') != 0 THEN TRIM(SUBSTR(DESCRIPTION,INSTR(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN'),'CATEGORIA DE')+LENGTH('CATEGORIA DE'),LENGTH(DESCRIPTION))) 
                     WHEN INSTR(upper(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'CATEGORIA') = 0 THEN 'SIN CATEGORIA'
                ELSE 'MANUAL' END AS CATEGORIA_PRODUCTO,
                'Bono electrónico' as TIPO_PRODUCTO,
                'INMOBILIARIA' as Linea_de_productos,
                'Producto' as TIPO,
                nvl(IS_EXTERNAL,'N') as Marca_externa
                FROM SIEBEL.CX_BRIEF_REQ 
    '''
    sb_qry_Bonos_2='''
                )Q_Inmerso
    '''
    qry_crit='''
    SELECT  
    ROW_ID as Brief_id,
    TRANSLATE(NAME,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') as NOMBRE_BRIEF,
    PRODUCT_CATEGORY as TipoMecanica,
    TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') AS MECANICA_DE_LA_PROMOCION,
    TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') as Restricciones,
    TRANSLATE(BENEFIT_DESC,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN') as DESCRIPCION_BENEFICIO,
    decode(PRODUCT_CATEGORY,'TROFEO 1','1 SIN CONDICION',
                            'TROFEO 2','2 SIN CONDICION',
                            'TROFEO 3','3 SIN CONDICION',
                            'TROFEO 4','4 SIN CONDICION',
                            'TROFEO 5','5 SIN CONDICION',
                            'TROFEO 6','6 SIN CONDICION',
                            'TROFEO 1 TICKET',1,
                            'TROFEO 2 TICKET',2,
                            'TROFEO 3 TICKET',3,
                            'TROFEO 4 TICKET',4,
                            'TROFEO 5 TICKET',5,
                            'TROFEO 6 TICKET',6,
                            'TROFEO 1 TICKET ACUMULADO',1,
                            'TROFEO 2 TICKET ACUMULADO',2,
                            'TROFEO 3 TICKET ACUMULADO',3,
                            'TROFEO 4 TICKET ACUMULADO',4,
                            'TROFEO 5 TICKET ACUMULADO',5,
                            'TROFEO 6 TICKET ACUMULADO',6,NULL) AS "TROFEO",
    CASE WHEN (INSTR(PRODUCT_CATEGORY,'TICKET') != 0 and INSTR(PRODUCT_CATEGORY,'ACUMULADO') = 0) then 1 else NULL end AS "TICKET MAXIMOS",
    Case when DESCRIPTION like '%MAYOR O IGUAL A%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'MAYOR O IGUAL A',1) + length('MAYOR O IGUAL A')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'MAYOR O IGUAL A',1) + length('MAYOR O IGUAL A'))+1) as numeric(20,2))-cast(INSTR(UPPER(DESCRIPTION),'MAYOR O IGUAL A',1) + length('MAYOR O IGUAL A') as numeric (20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%MAYOR A%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'MAYOR A',1) + length('MAYOR A')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'MAYOR A',1) + length('MAYOR A'))+1) as numeric(20,0))-cast(INSTR(UPPER(DESCRIPTION),'MAYOR A',1) + length('MAYOR A') as numeric(20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%MAYOR DE%'
     then SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'MAYOR DE',1) + length('MAYOR DE')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'MAYOR DE',1) + length('MAYOR DE'))+1) as numeric (20,0))-cast(INSTR(UPPER(DESCRIPTION),'MAYOR DE',1) + length('MAYOR DE') as numeric (20,0)))
     when DESCRIPTION like '%TICKET DE COMPRA POR%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA POR',1) + length('TICKET DE COMPRA POR')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA POR',1) + length('TICKET DE COMPRA POR'))+1) as numeric(20,0))-cast(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA POR',1) + length('TICKET DE COMPRA POR') as numeric (20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%TICKET DE COMPRA DE%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA DE',1) + length('TICKET DE COMPRA DE')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA DE',1) + length('TICKET DE COMPRA DE'))+1) as numeric(20,0))-cast(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA DE',1) + length('TICKET DE COMPRA DE') as numeric (20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%TICKET DE COMPRA%' and (DESCRIPTION not like '%TICKET DE COMPRA POR%' and DESCRIPTION not like '%TICKET DE COMPRA DE%')
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA',1) + length('TICKET DE COMPRA')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA',1) + length('TICKET DE COMPRA'))+1) as numeric (20,0))-cast(INSTR(UPPER(DESCRIPTION),'TICKET DE COMPRA',1) + length('TICKET DE COMPRA') as numeric(20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%TICKET POR%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'TICKET POR',1) + length('TICKET POR')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'TICKET POR',1) + length('TICKET POR'))+1) as numeric(20,0))-cast(INSTR(UPPER(DESCRIPTION),'TICKET POR',1) + length('TICKET POR') as numeric(20,0))),'$',''),',',''),' ','')
     when DESCRIPTION like '%TICKET DE%' and DESCRIPTION not like '%TICKET DE COMPRA%'
     then REPLACE(REPLACE(REPLACE(SUBSTR(UPPER(TRANSLATE(DESCRIPTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),(INSTR(UPPER(DESCRIPTION),'TICKET DE',1) + length('TICKET DE')),cast(INSTR(UPPER(DESCRIPTION),' ',(INSTR(UPPER(DESCRIPTION),'TICKET DE',1) + length('TICKET DE'))+1) as numeric(20,0))-cast(INSTR(UPPER(DESCRIPTION),'TICKET DE',1) + length('TICKET DE') as numeric(20,0))),'$',''),',',''),' ','')
    else null end as "MONTO MINIMO",
    NULL AS "CATEGORIA DE TIENDAS",
    CASE WHEN (PRODUCT_CATEGORY='GANADOR RS' or PRODUCT_CATEGORY='REDES SOCIALES') THEN 'REQUERIDA' ELSE NULL END AS "Participacion RS",
    CASE WHEN INSTR(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'VEZ') != 0 
          THEN DECODE(TRIM(REPLACE(REPLACE(replace(SUBSTR(trim(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ.','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN '))),(INSTR(UPPER(PROMO_RESTRICTION),'REDIMIR')+length('REDIMIR')),length('MAXIMO X')+1),'VEZ',''),'MAXIMO',''),'MAX','')),
          'UNA','1','SOLO UNA','1','DOS VECE','2','2 VE','2','1  POR','1','1  EL','1','1  PO','1','UNA SOLA','1',
          TRIM(REPLACE(REPLACE(replace(SUBSTR(trim(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ.','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN '))),(INSTR(UPPER(PROMO_RESTRICTION),'REDIMIR')+length('REDIMIR')),length('MAXIMO X')+1),'VEZ',''),'MAXIMO',''),'MAX','')))
     WHEN INSTR(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'VECES') != 0 
          THEN DECODE(TRIM(REPLACE(REPLACE(replace(SUBSTR(trim(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ.','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN '))),(INSTR(UPPER(PROMO_RESTRICTION),'REDIMIR')+length('REDIMIR')),length('MAXIMO X')+1),'VECES',''),'MAXIMO',''),'MAX','')),
          'UNA','1','SOLO UNA','1','DOS VECE','2','2 VE','2','1  POR','1','1  EL','1','1  PO','1','UNA SOLA','1',
          TRIM(REPLACE(REPLACE(replace(SUBSTR(trim(UPPER(TRANSLATE(PROMO_RESTRICTION,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ.','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN '))),(INSTR(UPPER(PROMO_RESTRICTION),'REDIMIR')+length('REDIMIR')),length('MAXIMO X')+1),'VECES',''),'MAXIMO',''),'MAX','')))
     ELSE '1'  END AS "Redenciones Maximas",
    CASE  WHEN (length(GALLERY) - length(replace(GALLERY,','))+1) = 1 
               THEN cast(BENEFIT_NUM as char (10))
          WHEN cast(SUBSTR(TRIM(BENEFIT_DESC),1,1) as char(10)) not in ('0','1','2','3','4','5','6','7','8','9') 
               then '999'
          WHEN (INSTR(UPPER(TRANSLATE(BENEFIT_DESC,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'POR CC') != 0 AND CAST(length(GALLERY) - length(replace(GALLERY,','))+1 AS INTEGER) > 1)
               OR (CAST(INSTR(UPPER(TRANSLATE(BENEFIT_DESC,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'POR CENTRO COMERCIAL') AS INTEGER) != 0 AND CAST(length(GALLERY) - length(replace(GALLERY,','))+1 AS INTEGER) > 1)
               OR (CAST(INSTR(UPPER(TRANSLATE(BENEFIT_DESC,'áéíóúàèìòùãõâêîôôäëïöüçñÁÉÍÓÚÀÈÌÒÙÃÕÂÊÎÔÛÄËÏÖÜÇÑ','aeiouaeiouaoaeiooaeioucnAEIOUAEIOUAOAEIOOAEIOUCN')),'POR CADA CENTRO COMERCIAL') AS INTEGER) != 0 AND CAST(length(GALLERY) - length(replace(GALLERY,','))+1 AS INTEGER) > 1)
               THEN cast(SUBSTR(TRIM(BENEFIT_DESC),1,INSTR(BENEFIT_DESC,' ')) as char(10))
          ELSE '999'
          END AS CANTIDAD_BENEFICIOS
    FROM SIEBEL.CX_BRIEF_REQ
    '''
    
    ##############################################FILTROS DE EXTRACCION DE LOS BRIEFS
    chosen_opt=2
        
    while chosen_opt not in [0,1]:
        chosen_opt=int(input('''
    SE RECOMIENDA HACER FILTROS POR LAS SIGUIENTES COLUMNAS:
                                        |ROW_ID  |NAME  |
                                        
    ============MENU [QUERY DE EXTRACCION SOBRE TABLA DE BRIEFs]=============================
    
    0.- EXTRACCION DE TODOS LOS BRIEF DE PROMOCIONES
    1.- AGREGANDO FILTROS 
    
    '''))
    
    qry_Bonos=''
    if chosen_opt == 0:
        qry_br_extractor=qry_br_extractor
        qry_Bonos=sb_qry_Bonos_1+sb_qry_Bonos_2
        qry_crit=qry_crit
    #    print(qry_br_extractor)
    else:
        var_filter=input('''TECLEA UN FILTRO SQL EX: NAME = 'NOMBRE_BRIEF' ''')
        qry_br_extractor=qry_br_extractor+' where '+var_filter
        qry_Bonos=sb_qry_Bonos_1+' where '+var_filter+sb_qry_Bonos_2
        qry_crit=qry_crit+' where '+var_filter
    #    print(qry_br_extractor)
    
    ##############################################
    print('Inicio de extraccion datos de la BD....')
    
    df_briefs=pd.read_sql(qry_br_extractor,db,index_col=None) #DataFrame contiene los Briefs de la consulta
    df_briefs.to_csv('Briefs_sucios.csv',index=False,enconding='latin-1') #SE EXTRAE EL DATAFRAME A UN CSV
        
    ##############################################TRABAJAR PRODUCTOS BONOS DE LOS BRIEFS
    c.execute(qry_Bonos)
    
    BRIEF_ID=[]
    BRIEF_NAME=[]
    ESTADO=[]
    NOMBRE_PRODUCTO=[]
    DESCRIPCION=[]
    BENEFICIOS_EXISTENTES=[]
    NO_BENEFICIOS_INICIALES=[]
    CATEGORIA_PRODUCTO=[]
    FECHA_INICIO_VIGENCIA=[]
    FECHA_FINAL_VIGENCIA=[]
    LINEA_PRODUCTOS=[]
    TIPO=[]
    PROVEEDOR=[]
    CATEGORIA=[]
    TIPO_PRODUCTO=[]
    MARCA_EXTERNA=[]
    DISPONIBLE_PARA_PEDIDO=[]
    for i in c:
          BRIEF_ID+=[str(i[0])]
          BRIEF_NAME+=[str(i[1]).replace('\n','').replace('\t','').replace('\r','')]
          ESTADO+=[str(i[2]).replace('\n','').replace('\t','').replace('\r','')]
          NOMBRE_PRODUCTO+=[str(i[3]).replace('\n','').replace('\t','').replace('\r','')]
          DESCRIPCION+=[str(i[4]).replace('\n','').replace('\t','').replace('\r','')]
          BENEFICIOS_EXISTENTES+=[str(i[5]).replace('\n','').replace('\t','').replace('\r','')]
          NO_BENEFICIOS_INICIALES+=[str(i[6]).replace('\n','').replace('\t','').replace('\r','')]
          CATEGORIA_PRODUCTO+=[str(i[7]).replace('\n','').replace('\t','').replace('\r','')]
          FECHA_INICIO_VIGENCIA+=[str(i[8]).replace('\n','').replace('\t','').replace('\r','')]
          FECHA_FINAL_VIGENCIA+=[str(i[9]).replace('\n','').replace('\t','').replace('\r','')]
          LINEA_PRODUCTOS+=[str(i[10]).replace('\n','').replace('\t','').replace('\r','')]
          TIPO+=[str(i[11]).replace('\n','').replace('\t','').replace('\r','')]
          PROVEEDOR+=[str(i[12]).replace('\n','').replace('\t','').replace('\r','')]
          CATEGORIA+=[str(i[13]).replace('\n','').replace('\t','').replace('\r','')]
          TIPO_PRODUCTO+=[str(i[14]).replace('\n','').replace('\t','').replace('\r','')]
          MARCA_EXTERNA+=[str(i[15]).replace('\n','').replace('\t','').replace('\r','')]
          DISPONIBLE_PARA_PEDIDO+=['F']
    
    
    dict_bonos={
          'BRIEF_ID':pd.Series(BRIEF_ID),
          'BRIEF_NAME':pd.Series(BRIEF_NAME),
          'ESTADO':pd.Series(ESTADO),
          'NOMBRE_PRODUCTO':pd.Series(NOMBRE_PRODUCTO),
          'DESCRIPCION':pd.Series(DESCRIPCION),
          'BENEFICIOS_EXISTENTES':pd.Series(BENEFICIOS_EXISTENTES),
          'NO_BENEFICIOS_INICIALES':pd.Series(NO_BENEFICIOS_INICIALES),
          'CATEGORIA_PRODUCTO':pd.Series(CATEGORIA_PRODUCTO),
          'FECHA_INICIO_VIGENCIA':pd.Series(FECHA_INICIO_VIGENCIA),
          'FECHA_FINAL_VIGENCIA':pd.Series(FECHA_FINAL_VIGENCIA),
          'LINEA_PRODUCTOS':pd.Series(LINEA_PRODUCTOS),
          'TIPO':pd.Series(TIPO),
          'PROVEEDOR':pd.Series(PROVEEDOR),
          'CATEGORIA':pd.Series(CATEGORIA),
          'TIPO_PRODUCTO':pd.Series(TIPO_PRODUCTO),
          'MARCA_EXTERNA':pd.Series(MARCA_EXTERNA),
          'DISPONIBLE_PARA_PEDIDO':pd.Series(DISPONIBLE_PARA_PEDIDO)
          }
    
    bonos=pd.DataFrame(dict_bonos)
    bonos.loc[bonos['BRIEF_NAME']=='None','BRIEF_NAME']=''
    bonos.loc[bonos['NOMBRE_PRODUCTO']=='None','NOMBRE_PRODUCTO']=''
    bonos.loc[bonos['DESCRIPCION']=='None','DESCRIPCION']=''
    bonos.loc[bonos['BENEFICIOS_EXISTENTES']=='None','BENEFICIOS_EXISTENTES']=''
    bonos.loc[bonos['CATEGORIA_PRODUCTO']=='None','CATEGORIA_PRODUCTO']=''
    bonos.loc[bonos['NO_BENEFICIOS_INICIALES']=='None','NO_BENEFICIOS_INICIALES']=''
    bonos.loc[bonos['FECHA_INICIO_VIGENCIA']=='None','FECHA_INICIO_VIGENCIA']=''
    bonos.loc[bonos['FECHA_FINAL_VIGENCIA']=='None','FECHA_FINAL_VIGENCIA']=''
    bonos.loc[bonos['TIPO']=='None','TIPO']=''
    bonos.loc[bonos['PROVEEDOR']=='None','PROVEEDOR']=''
    bonos.loc[bonos['CATEGORIA']=='None','CATEGORIA']=''
    bonos.loc[bonos['TIPO_PRODUCTO']=='None','TIPO_PRODUCTO']=''
    bonos.loc[bonos['MARCA_EXTERNA']=='None','MARCA_EXTERNA']=''
    bonos[['BRIEF_ID','ESTADO','NOMBRE_PRODUCTO','DESCRIPCION','NO_BENEFICIOS_INICIALES','BENEFICIOS_EXISTENTES',
           'CATEGORIA_PRODUCTO','FECHA_INICIO_VIGENCIA','FECHA_FINAL_VIGENCIA','TIPO','PROVEEDOR','CATEGORIA','TIPO_PRODUCTO',
           'LINEA_PRODUCTOS','DISPONIBLE_PARA_PEDIDO']].to_csv('Productos_Bono.csv',index=False,enconding='latin-1')    
        
        
    ##############################################TRABAJAR CRITERIOS POR CADA BRIEF
    
    c.execute(qry_crit)
    
    BRIEF_ID=[]
    NOMBRE_BRIEF=[]
    TipoMecanica=[]
    MECANICA_DE_LA_PROMOCION=[]
    Restricciones=[]
    DESCRIPCION_BENEFICIO=[]
    TROFEO=[]
    TICKET_MAXIMOS=[]
    MONTO_MINIMO=[]
    CATEGORIA_TIENDAS=[]
    Participacion_RS=[]
    Redenciones_Maximas=[]
    CANTIDAD_BENEFICIOS=[]
    
    for  i in c:
          BRIEF_ID+=[str(i[0])]
          NOMBRE_BRIEF+=[str(i[1]).replace('\n','').replace('\t','').replace('\r','')]
          TipoMecanica+=[str(i[2]).replace('\n','').replace('\t','').replace('\r','')]
          MECANICA_DE_LA_PROMOCION+=[str(i[3]).replace('\n','').replace('\t','').replace('\r','')]
          Restricciones+=[str(i[4]).replace('\n','').replace('\t','').replace('\r','')]
          DESCRIPCION_BENEFICIO+=[str(i[5]).replace('\n','').replace('\t','').replace('\r','')]
          TROFEO+=[str(i[6]).replace('\n','').replace('\t','').replace('\r','')]
          TICKET_MAXIMOS+=[str(i[7]).replace('\n','').replace('\t','').replace('\r','')]
          MONTO_MINIMO+=[str(i[8]).replace('\n','').replace('\t','').replace('\r','')]
          CATEGORIA_TIENDAS+=[str(i[9]).replace('\n','').replace('\t','').replace('\r','')]
          Participacion_RS+=[str(i[10]).replace('\n','').replace('\t','').replace('\r','')]
          Redenciones_Maximas+=[str(i[11]).replace('\n','').replace('\t','').replace('\r','')]
          CANTIDAD_BENEFICIOS+=[str(i[12]).replace('\n','').replace('\t','').replace('\r','')]
    
    dict_forma_criterio_1={
          'BRIEF_ID':pd.Series(BRIEF_ID),
          'NOMBRE_BRIEF':pd.Series(NOMBRE_BRIEF),
          'TipoMecanica':pd.Series(TipoMecanica),
          'MECANICA_DE_LA_PROMOCION':pd.Series(MECANICA_DE_LA_PROMOCION),
          'Restricciones':pd.Series(Restricciones),
          'DESCRIPCION_BENEFICIO':pd.Series(DESCRIPCION_BENEFICIO),
          'TROFEO':pd.Series(TROFEO),
          'TICKET_MAXIMOS':pd.Series(TICKET_MAXIMOS),
          'MONTO_MINIMO':pd.Series(MONTO_MINIMO),
          'CATEGORIA_TIENDAS':pd.Series(CATEGORIA_TIENDAS),
          'Participacion_RS':pd.Series(Participacion_RS),
          'Redenciones_Maximas':pd.Series(Redenciones_Maximas),
          'CANTIDAD_BENEFICIOS':pd.Series(CANTIDAD_BENEFICIOS)
          }
    forma_criterio_1=pd.DataFrame(dict_forma_criterio_1)
    forma_criterio_1.loc[forma_criterio_1['TROFEO']=='None','TROFEO']=''
    forma_criterio_1.loc[forma_criterio_1['TICKET_MAXIMOS']=='None','TICKET_MAXIMOS']=''
    forma_criterio_1.loc[forma_criterio_1['MONTO_MINIMO']=='None','MONTO_MINIMO']=''
    forma_criterio_1.loc[forma_criterio_1['CATEGORIA_TIENDAS']=='None','CATEGORIA_TIENDAS']=''
    forma_criterio_1.loc[forma_criterio_1['Participacion_RS']=='None','Participacion_RS']=''
    forma_criterio_1.loc[forma_criterio_1['Redenciones_Maximas']=='None','Redenciones_Maximas']=''
    
    forma_criterio_1[['BRIEF_ID','NOMBRE_BRIEF','TipoMecanica','MECANICA_DE_LA_PROMOCION','Restricciones',
                                  'DESCRIPCION_BENEFICIO','TROFEO','TICKET_MAXIMOS','MONTO_MINIMO','CATEGORIA_TIENDAS',
                                  'Participacion_RS','Redenciones_Maximas','CANTIDAD_BENEFICIOS']].to_csv("BASE_CRITERIOS.csv",index=False)
                                  
    c.close()
    print('Se finalizao la extraccion de datos')
    return()
    
if __name__ == "__main__":
    queries_base()                              

