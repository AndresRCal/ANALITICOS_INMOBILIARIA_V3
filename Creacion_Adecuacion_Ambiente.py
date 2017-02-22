# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import time
import shutil
import queries as qry
import desmenuzar_briefs_V2 as dzar
import Salidas as sals
import en_lugarde_qry as e_qry

fecha=time.strftime("%x")
hora= time.strftime("%X")
Base_dir=os.getcwd()
Base_Root='''D:\\soporte inmobi\\Promos\\Pase_a_produc\\'''
TEMP_DESLLO=input("TEMPORADA A DESARROLLAR ANALITICOS?")
print (TEMP_DESLLO)
New_Base_Root=Base_Root+TEMP_DESLLO+'\\'
print(New_Base_Root)
if os.path.exists(New_Base_Root) == True:
    os.chdir(New_Base_Root)
    print("El Directorio ya existe!...")
else:
    os.makedirs(New_Base_Root)
    os.chdir(New_Base_Root)

dir_cns=0
Work_Root=New_Base_Root+'\\'+str(fecha).replace('/','_')+'_'+TEMP_DESLLO+'.V'+str(dir_cns)
if os.path.exists(Work_Root) == True:
    while True:
        Work_Root_aux=Work_Root.split('.V')
        dir_cns=int(Work_Root_aux[1].replace('.V','').replace('V',''))
        dir_cns=dir_cns+1
        Work_Root=Work_Root_aux[0]+'.V'+str(dir_cns)
        if os.path.exists(Work_Root) == True:
            continue
        else:
            os.makedirs(Work_Root)
            break
else:
    os.makedirs(Work_Root)
    
Cataloges_Root=New_Base_Root+os.sep+'Catalogos_base'+os.sep
if not os.path.exists(Cataloges_Root):
    shutil.copytree(Base_dir+os.sep+"Catalogos_base"+os.sep,"Catalogos_base")
    
chosen_source=''    
while chosen_source not in ['0','1']:
    chosen_source=input(
'''
¿Cual es la fuente de los analiticos a calcular?: 
    
    0    QUERY A BASE DE DATOS
    1    ARCHIVO PROVISTO POR NEGOCIO
    
*TECLEA 0 o 1

''')

if chosen_source == '0':
    qry.queries_base(Work_Root)
elif chosen_source == '1':
    e_qry.establece_laBase(Base_dir,Work_Root)
else:
    print('HUBO UN ERROR LOS IDENTIFICADORES VALIDOS PARA LAS FUENTES SON SOLO 0 y 1')
    print('SE TOMA FUENTE DEFAULT: 0')
    qry.queries_base(Work_Root)

dzar.desmenuzar_brief(Work_Root,Cataloges_Root)

chosen_opt=''
while chosen_opt.upper() not in ['S','N']:
    chosen_opt=input('''

			En este punto es imperativo el revisar los archivos creados: 
			
			1) BASE_CRITERIOS.csv
			2) Productos_Bono.csv
			3) Brief_desmenuzado.csv
			
			Modifica o completa la informacion faltante dentro de los archivos,
			al terminar guarda los cambios sin modificar el formato del archivo (csv)
			
			Ahora por favor ve y checa!!!!!!
			
			Una vez corregida y/o añadida toda informacion faltante.
			Deseas continuar?(S/N)
''')

if chosen_opt.upper() == 'S':
    print('Continuamos entonces...')
    print('Calculando los analiticos de las promociones')
    dzar.Calculo_NPromoProd(Work_Root)
    sals.analiticos_promocionInmo(Work_Root)
    print('Calculando los analiticos de los productos de fideliacion')
    sals.analiticosproductos_fidelizacion(Work_Root)
    print('Muchas gracias el proceso ha finalizado!')
    exit()
if chosen_opt.upper() == 'N':
    print('''
    De acuerdo. Para finalizar los analiticos después, deberas correr el 
    siguiente programa: Creacion_Adeacuacion_Ambiente_retomado.py el cual 
    esta en la carpeta local de la temporada a sacar los analiticos.
    ''')
    os.chdir(Work_Root+os.sep)
    if os.path.isfile('Briefs_sucios.csv') and os.path.isfile('BASE_CRITERIOS.csv') and os.path.isfile('Productos_Bono.csv') and os.path.isfile('Brief_desmenuzado.csv'):
        os.chdir(Base_dir)
        if os.path.isfile('Salidas.py') == True:
            shutil.copy2("Salidas.py",Work_Root+os.sep+"Creacion_Adeacuacion_Ambiente_retomado.py")
            shutil.copy2("desmenuzar_briefs_V2.py",Work_Root+os.sep+"desmenuzar_briefs_V2.py")
            print('Se copio el archivo Creacion_Adeacuacion_Ambiente_retomado.py a tu carpeta de trabajo con exito: \n',Work_Root+os.sep)
        else:
            print('No se puede encontrar el archivo Salidas.py dentro de la ruta: ',Base_dir) 
    else:
        print('No se encuentran los archivos suficientes para poder proseguir en la carpeta: ',Work_Root+os.sep)

os.chdir(Base_dir)
