import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date

def processArchive():
    fields = ['Projeto','Observação','Regional','OC_NetFlow','TSSR','TSSR Data','RF SHEET','RF SHEET Data','CDD','CDD Data','INITIAL TUNNING','INITIAL TUNNING Data']
    fields2 = ['Projeto','Obs','Regional','OC_NetFlow','TSSR','TSSR Data','RF SHEET','RF SHEET Data','CDD','CDD Data','INITIAL TUNNING','INITIAL TUNNING Data']
    pathImport = '/import/Licceu'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/Licceu/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.xlsx")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        #data = pd.read_excel(filename,skiprows=27,sheet_name = 'DUMP_5G_DSS', nrows=40,usecols = 'A:AC')
        data = pd.read_excel(filename,sheet_name = 'Plano Operativo Acesso',usecols = fields)
        df = df.append(data,ignore_index=True)
        df = df[fields] # ordering labels
        li.append(df)  
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.loc[frameSI['Regional'] == 'TSP']

    DataList = ['TSSR Data','RF SHEET Data','CDD Data','INITIAL TUNNING Data']
    DataList2 = []
    for i in DataList:
      frameSI[i] = pd.to_datetime(frameSI[i], format="%d/%m/%Y")
      frameSI.loc[~frameSI[i].isna(),[i[:-5]+'_Status']] = 'OK'
      if i[:-5]+'_Status' not in DataList2:
        DataList2.append(i[:-5]+'_Status')

    print(DataList2)

    
    frameSI.insert(len(frameSI.columns),'Pending','')
    frameSI.insert(len(frameSI.columns),'STATUS','')
    
    for index, row in frameSI.iterrows():
      logError = []
      for i in DataList2:
        if row[i] != 'OK':
          logError.append(i.split('_')[0]+'|')
      print(logError)

      if len(logError) > 0:
        s = ''.join(str(x) for x in logError)
        frameSI.at[index,'Pending'] = s[:-1]
        frameSI.at[index,'STATUS'] = 'NOT OK'
      else:
        frameSI.at[index,'Pending'] = 'NO'
        frameSI.at[index,'STATUS'] = 'OK'
    #BlackList
    #Ampliacao MOCN não necessita documentao
    frameSI.loc[frameSI['Projeto'].str.contains('MOCN'),['STATUS']] = 'OK'

    #SLS verificar apenas: RFSHEET|CDD     'RF SHEET|CDD'
    frameSI.loc[(frameSI['Obs'].str.contains('SLS')) & (~frameSI['Pending'].str.contains('TSSR|RF SHEET|CDD')),['STATUS']] = 'OK'
    
    print(frameSI)
    

    #726778

    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')


