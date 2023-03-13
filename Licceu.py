import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import time
import TratarArquivo

def processArchive():
    fields = ['Projeto','Classe de site','Observação','Regional','OC_NetFlow','TSSR','TSSR Data','RF SHEET','RF SHEET Data','CDD','CDD Data','INITIAL TUNNING','INITIAL TUNNING Data']
    fields2 = ['Projeto','ClasseSite','Obs','Regional','OC_NetFlow','TSSR','TSSR Data','RF SHEET','RF SHEET Data','CDD','CDD Data','INITIAL TUNNING','INITIAL TUNNING Data','DateArchive']
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
        dataFile = time.strftime('%Y%m%d', time.gmtime(os.path.getmtime(filename)))
        data = pd.read_excel(filename,sheet_name = 'Plano Operativo Acesso',usecols = fields)
        df = df.append(data,ignore_index=True)
        df = df[fields] # ordering labels
        df['DateArchive'] = dataFile
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

    #print(DataList2)

    
    frameSI.insert(len(frameSI.columns),'Pending','')
    frameSI.insert(len(frameSI.columns),'STATUS','')
    
    for index, row in frameSI.iterrows():
      logError = []
      for i in DataList2:
        if row[i] != 'OK':
          logError.append(i.split('_')[0]+'|')
      #print(logError)

      if len(logError) > 0:
        s = ''.join(str(x) for x in logError)
        frameSI.at[index,'Pending'] = s[:-1]
        frameSI.at[index,'STATUS'] = 'NOT OK'
      else:
        frameSI.at[index,'Pending'] = 'OK'
        frameSI.at[index,'STATUS'] = 'OK'

        
    #BlackList
    #frameSI = frameSI.loc[frameSI['ClasseSite'] != 'WI-FI']
    #frameSI = TratarArquivo.processArchive(frameSI)

    '''
    #Ampliacao MOCN não necessita documentao
    frameSI.loc[frameSI['Projeto'].str.contains('MOCN'),['STATUS']] = 'OK'
    #SLS verificar apenas: RFSHEET|CDD     'RF SHEET|CDD'
    frameSI.loc[(frameSI['Obs'].str.contains('SLS')) & (~frameSI['Pending'].str.contains('TSSR|RF SHEET|CDD')),['STATUS']] = 'OK'
    frameSI.loc[(frameSI['Projeto'].str.contains('REMANEJAMENTO')) & (~frameSI['Pending'].str.contains('TSSR|RF SHEET|CDD|INITIAL TUNNING')),['STATUS']] = 'TIM'
    '''
    
    #print(frameSI)
     

    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')


