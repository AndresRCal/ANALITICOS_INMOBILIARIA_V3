# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import alGusto as alg
import pandas as pd

#==============================================================================
# FUNCIONES DE UTILERIA
#==============================================================================


def nombreBono_deBRIEF(nombreBr):
    vls=alg.accent_clean(nombreBr)
    vls=vls.upper().strip().split('-')
    s3=[vls[len(vls)-2].strip(),vls[len(vls)-1].strip()]
    s3=alg.concat_hyphen(s3)
    return(s3)

def nombreBono_Formado(prov,prod):
    try:
        return(str(alg.accent_clean(prov)).upper().strip(' ').replace(',','').replace("'","")+' - '+str(alg.accent_clean(prod)).upper().strip(' ').replace(',','').replace("'",""))
    except Exception as msg:
        print(msg)
        return(str(prov).upper().strip(' ').replace(',','').replace("'","")+' - '+str(prod).upper().strip(' ').replace(',','').replace("'",""))

def categ_beneficio(Mecanica_de_la_promocion):
    cat_x_br='I-BNO-SIN CATEGORIA'
    catg_bene=['COME DELICIOSO','CONOCE EL MUNDO', 'DIVIÉRTETE EN GRANDE', 'ESTRÉNALO', 'FESTEJAMOS CONTIGO', 'GANA CON ESTILO', 'HOGAR DULCE HOGAR', 'MOMENTOS DELICIOSOS', 'MOMENTOS ÚNICOS', 'PARA CONSENTIRTE', 'PARA REGALAR', 'PREPÁRATE', 'PROMOCIONES', 'SÓLO ESTRENOS', 'SÓLO PARA PEQUEÑOS', 'TE LLEVAMOS A', 'TU ESTILO', 'SIN CATEGORIACOME DELICIOSO', 'CONOCE EL MUNDO', 'DIVIERTETE EN GRANDE', 'ESTRENALO', 'FESTEJAMOS CONTIGO', 'GANA CON ESTILO', 'HOGAR DULCE HOGAR', 'MOMENTOS DELICIOSOS', 'MOMENTOS UNICOS', 'PARA CONSENTIRTE', 'PARA REGALAR', 'PREPARATE', 'PROMOCIONES', 'SOLO ESTRENOS', 'SOLO PARA PEQUEÑOS', 'TE LLEVAMOS A', 'TU ESTILO', 'SIN CATEGORIA']
    for j in catg_bene:
            if alg.accent_clean(j) in alg.accent_clean(Mecanica_de_la_promocion):
                cat_x_br='I-BNO-'+j
            else:
                pass
    return(cat_x_br)

def extraccion_numeros(cad):
    entrada=cad
    salida_num=''
    nums=['0','1','2','3','4','5','6','7','8','9']
    for i in entrada:
        if i in nums:
            salida_num+=i
    return(salida_num)

def decode_likeOracle(cad):
    salida=''
    if 'TROFEO' in cad:
        if 'TICKET' not in cad:
            salida=str(extraccion_numeros(cad))+' SIN CONDICION'
        else:
            salida=str(extraccion_numeros(cad))
    return(salida)

def colm_crit_ticketMax(cad):
    if 'TICKET' in cad:
        if 'ACUMULADO' not in cad:
            return(1)
        else:
            return('')
    else:
        return('')
    
def quita_sentencias_conocidas(cad):
    try:
        return(cad.upper().replace('TROFEO 1','').replace('TROFEO 2','').replace('TROFEO 3','').replace('TROFEO 4','').replace('TROFEO 5','').replace('TROFEO 6','').replace('TROFEO1','').replace('TROFEO2','').replace('TROFEO3','').replace('TROFEO4','').replace('TROFEO5','').replace('TROFEO6','').replace('.00','').replace('.0',''))
    except Exception as msg:
        print(msg)
        return(cad)

def colm_partRS(cad):
    if cad == 'GANADOR RS' or cad == 'REDES SOCIALES':
        return('REQUERIDA')
    else:
        return('')
    
#==============================================================================
# FUNCIONES DE CONSTRUCCION DE DATAFRAMES
#==============================================================================

def df_briefs(df_negocio):
    Brief_id=[]
    NOMBRE_BRIEF=[]
    MECANICA_DE_LA_PROMOCION=[]
    Galerias=[]
    FechaInicial=[]
    FechasFinal=[]
    Proveedor=[]
    Beneficio=[]
    NumDeBeneficios=[]
    DescripcionDelBeneficio=[]
    Categoria=[]
    Restricciones=[]
    TipoDeMecanica=[]
    Cantidad_Cupones=[]
    Restriccion_Cupones=[]
    Marca_externa=[]
    for i in df_negocio.index:
        Brief_id+=[str(alg.accent_clean(df_negocio['Nombre de la promoción'][i])).upper().strip(' ').replace('\t','').replace('\n','').replace('\r','').replace(' ','').replace('.','').replace(',','').replace('-','').replace('´','').replace('$','').replace('%','').replace("'","")]
        NOMBRE_BRIEF+=[str(alg.accent_clean(df_negocio['Nombre de la promoción'][i])).replace('\n','').replace('\t','').replace('\r','')]
        MECANICA_DE_LA_PROMOCION+=[str(alg.accent_clean(df_negocio['Mecanica de la promocion'][i])).upper().strip(' ').replace('\t',' ').replace('\n','').replace('\r','')]
        Galerias+=[str(alg.accent_clean(df_negocio['Galerias'][i])).upper().strip(' ').strip('\t').strip('\n').strip('\r').replace('G.','').replace('GALERIAS','').replace('G ','')]
        FechaInicial+=[alg.string_to_date(df_negocio['Inicio de la promoción'][i])]
        FechasFinal+=[alg.string_to_date(df_negocio['Fin de la promoción'][i])]
        Proveedor+=[str(alg.accent_clean(df_negocio['Proveedor'][i])).upper().strip(' ').replace('\t',' ').replace('\n','').replace('\r','')]
        Beneficio+=[str(alg.accent_clean(df_negocio['Beneficio'][i])).upper().strip(' ').strip('\t').strip('\n').strip('\r').replace('\t',' ').replace('\n','').replace('\r','').replace('.','').replace(',','')]
        NumDeBeneficios+=[df_negocio['Num. de Beneficios'][i]]
        DescripcionDelBeneficio+=[str(alg.accent_clean(df_negocio['Descripcion del Beneficio'][i])).upper().strip(' ').strip('\t').strip('\n').strip('\r').replace('\t',' ').replace('\n','').replace('\r','')]
        Categoria+=[df_negocio['Categoría'][i]]
        Restricciones+=[str(alg.accent_clean(df_negocio['Restricciones'][i])).upper().strip(' ').strip('\t').strip('\n').strip('\r').replace('\t',' ').replace('\n','').replace('\r','')]
        TipoDeMecanica+=[str(alg.accent_clean(df_negocio['Tipo de Mecanica'][i]))]
        Cantidad_Cupones+=[df_negocio['Num. de cupones'][i]]
        Restriccion_Cupones+=[df_negocio['Restricciones del cupon'][i]]
        Marca_externa+=[str(df_negocio['Marca externa'][i]).upper()]
    dict_brief={
            'BRIEF_ID':Brief_id,
            'NOMBRE_BRIEF':NOMBRE_BRIEF,
            'MECANICA_DE_LA_PROMOCION':MECANICA_DE_LA_PROMOCION,
            'GALERIAS':Galerias,
            'FECHAINICIAL':FechaInicial,
            'FECHASFINAL':FechasFinal,
            'PROVEEDOR':Proveedor,
            'BENEFICIO':Beneficio,
            'NumDeBeneficios':NumDeBeneficios,
            'DescripcionDelBeneficio':DescripcionDelBeneficio,
            'Categoria':Categoria,
            'Restricciones':Restricciones,
            'TIPODEMECANICA':TipoDeMecanica,
            'Cantidad_Cupones':Cantidad_Cupones,
            'Restriccion_Cupones':Restriccion_Cupones,
            'Marca_externa':Marca_externa
            }
    salida=pd.DataFrame(dict_brief)
    return(salida)

def df_BONOS(df_negocio):
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
    for i in df_negocio.index:
        BRIEF_ID+=[df_negocio['BRIEF_ID'][i]]
        BRIEF_NAME+=[df_negocio['NOMBRE_BRIEF'][i]]
        ESTADO+=['Activo']
        NOMBRE_PRODUCTO+=[nombreBono_Formado(df_negocio['PROVEEDOR'][i],df_negocio['BENEFICIO'][i])]
        DESCRIPCION+=['']
        BENEFICIOS_EXISTENTES+=['']
        NO_BENEFICIOS_INICIALES+=['']
        CATEGORIA_PRODUCTO+=['']
        FECHA_INICIO_VIGENCIA+=['']
        FECHA_FINAL_VIGENCIA+=['']
        LINEA_PRODUCTOS+=['INMOBILIARIA']
        TIPO+=['Producto']
        PROVEEDOR+=[df_negocio['PROVEEDOR'][i]]
        CATEGORIA+=[categ_beneficio(df_negocio['MECANICA_DE_LA_PROMOCION'][i])]
        TIPO_PRODUCTO+=['Bono electrónico']
        MARCA_EXTERNA+=[df_negocio['Marca_externa'][i]]
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
    bonos=pd.DataFrame(dict_bonos).fillna('')
    bonos.loc[bonos['MARCA_EXTERNA']=='None','MARCA_EXTERNA']=''
    bonos=bonos[['BRIEF_ID','ESTADO','NOMBRE_PRODUCTO','DESCRIPCION','NO_BENEFICIOS_INICIALES','BENEFICIOS_EXISTENTES',
           'CATEGORIA_PRODUCTO','FECHA_INICIO_VIGENCIA','FECHA_FINAL_VIGENCIA','TIPO','PROVEEDOR','CATEGORIA','TIPO_PRODUCTO',
           'LINEA_PRODUCTOS','DISPONIBLE_PARA_PEDIDO']]
    return(bonos)

def df_crits(df_negocio):
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
    for i in df_negocio.index:
        BRIEF_ID+=[df_negocio['BRIEF_ID'][i]]
        NOMBRE_BRIEF+=[df_negocio['NOMBRE_BRIEF'][i]]
        TipoMecanica+=[df_negocio['TIPODEMECANICA'][i]]
        MECANICA_DE_LA_PROMOCION+=[df_negocio['MECANICA_DE_LA_PROMOCION'][i]]
        Restricciones+=[df_negocio['Restricciones'][i]]
        DESCRIPCION_BENEFICIO+=[df_negocio['DescripcionDelBeneficio'][i]]
        TROFEO+=[decode_likeOracle(df_negocio['TIPODEMECANICA'][i]).strip()]
        TICKET_MAXIMOS+=[colm_crit_ticketMax(df_negocio['TIPODEMECANICA'][i])]
        if 'TICKET' in df_negocio['TIPODEMECANICA'][i]:
            MONTO_MINIMO+=[extraccion_numeros(quita_sentencias_conocidas(df_negocio['MECANICA_DE_LA_PROMOCION'][i]))]
        else:
            MONTO_MINIMO+=['']
        CATEGORIA_TIENDAS+=['']
        Participacion_RS+=[colm_partRS(df_negocio['TIPODEMECANICA'][i])]
        Redenciones_Maximas+=['1']
        CANTIDAD_BENEFICIOS+=[df_negocio['NumDeBeneficios'][i]]
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
                                      'Participacion_RS','Redenciones_Maximas','CANTIDAD_BENEFICIOS']]
    return(forma_criterio_1)

    

def establece_laBase(Base_dir,Work_Root):
    #==============================================================================
    # SOLICITO UN ARCHIVO EXCEL Y UN NOMBRE DE HOJA Y ME ASEGURO DE QUE REALMENTE EXISTE
    #==============================================================================
    band_arch='F'
#    print(band_arch)
    try:
        while band_arch=='F':
            
            
            arch_negocio=str(input('''Dame el nombre del archivo provisto por Negocio a trabajar los analiticos:
                '''))
#            arch_negocio='PROMOCIONES ATEMPORALES 22_03_2017.xlsx'
                
            sheet_name=str(input('''Nececito el nombre de la hoja con los briefs:
                '''))
#            sheet_name='Sheet1'  
               
            print(Base_dir+os.sep+arch_negocio)
            if os.path.exists(Base_dir+os.sep+arch_negocio) == True:
                band_arch='V'
                df_negocio=alg.Rexcel_wpd(Base_dir,arch_negocio,sheet_name)
            else:
                print('''No se pudo encontrar el archivo''')
        
#        print('salio')
    except Exception as msg:
        print(msg)
    #==============================================================================
    # BRIEFS
    #==============================================================================
    df_negocio=df_briefs(df_negocio)
    try:
        df_negocio.to_csv(Work_Root+os.sep+'Briefs_sucios.csv',index=False,enconding='latin-1') #SE EXTRAE EL DATAFRAME A UN CSV        
    except Exception as msg:
        print(msg)
        df_negocio.to_csv(Work_Root+os.sep+'Briefs_sucios.csv',index=False) #SE EXTRAE EL DATAFRAME A UN CSV        
    #==============================================================================
    # BONOS
    #==============================================================================
    df_bonos=df_BONOS(df_negocio)
    try:
        df_bonos[['BRIEF_ID','ESTADO','NOMBRE_PRODUCTO','DESCRIPCION','NO_BENEFICIOS_INICIALES','BENEFICIOS_EXISTENTES',
                   'CATEGORIA_PRODUCTO','FECHA_INICIO_VIGENCIA','FECHA_FINAL_VIGENCIA','TIPO','PROVEEDOR','CATEGORIA','TIPO_PRODUCTO',
                   'LINEA_PRODUCTOS','DISPONIBLE_PARA_PEDIDO']].to_csv(Work_Root+os.sep+'Productos_Bono.csv',index=False,enconding='latin-1') 
    except Exception as msg:
        print(msg)
        df_bonos[['BRIEF_ID','ESTADO','NOMBRE_PRODUCTO','DESCRIPCION','NO_BENEFICIOS_INICIALES','BENEFICIOS_EXISTENTES',
                   'CATEGORIA_PRODUCTO','FECHA_INICIO_VIGENCIA','FECHA_FINAL_VIGENCIA','TIPO','PROVEEDOR','CATEGORIA','TIPO_PRODUCTO',
                   'LINEA_PRODUCTOS','DISPONIBLE_PARA_PEDIDO']].to_csv(Work_Root+os.sep+'Productos_Bono.csv',index=False) 
    #==============================================================================
    # CRITERIOS
    #==============================================================================
    df_cirterios=df_crits(df_negocio)
    df_cirterios[['BRIEF_ID','NOMBRE_BRIEF','TipoMecanica','MECANICA_DE_LA_PROMOCION','Restricciones',
                                          'DESCRIPCION_BENEFICIO','TROFEO','TICKET_MAXIMOS','MONTO_MINIMO','CATEGORIA_TIENDAS',
                                          'Participacion_RS','Redenciones_Maximas','CANTIDAD_BENEFICIOS']].to_csv(Work_Root+os.sep+"BASE_CRITERIOS.csv",index=False)
    return()


