import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
from datetime import datetime
import statistics




def processArchive():
    fields = ['PLANO','ORDEM COMPLEXA','STATUS OC','ELEMENTO ID','FREQUÊNCIA ROLLOUT','ENDEREÇO ID','CLASSE SITE','CLASSIFICAÇÃO CASA','TECNOLOGIA','REGIONAL','UF','ANF','COD.IBGE','TIPO SITE','ETAPA ATUAL','TIPO ELEMENTO','STATUS FINANCEIRO','#BBU','REAL ATIVAÇÃO NETFLOW']
    fields2 = ['PLANO','ORDEM_COMPLEXA','STATUS_OC','ELEMENTO_ID','FREQUENCIA_ROLLOUT','ENDERECO_ID','CLASSE_SITE','CLASSIFICACAO_CASA','TECNOLOGIA','REGIONAL','UF','ANF','COD_IBGE','TIPO_SITE','ETAPA_ATUAL','TIPO_ELEMENTO','STATUS_FINANCEIRO','#BBU','REAL_ATIVACAO_NETFLOW']
    
    pathImport = '/import/PMO'

    pathImportSI = os.getcwd() + pathImport
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/PMO/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    lastData = all_filesSI[0][len(all_filesSI[0])-19:len(all_filesSI[0])-11]
    for filename in all_filesSI:
        dataArchive = filename[len(pathImportSI)+14:len(filename)-11]
        iter_csv = pd.read_csv(filename, index_col=None,header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI.loc[(frameSI['REAL_ATIVACAO_NETFLOW'] != '-') & (frameSI['STATUS_OC'] !='CLOSED'),['STATUS_OC']] ='REAL_ATIVACAO_NETFLOW'
    frameSI = frameSI.drop_duplicates()
    frameSI.to_csv(csv_path,index=False,header=True,sep=';')


