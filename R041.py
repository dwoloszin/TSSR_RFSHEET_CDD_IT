import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date

def processArchive():
    fields = ['ANF','Ordem Complexa','Nome Anexo','Tipo Anexo','Data Criação','Usuário Criação Anexo','Data Atualização','Usuário Atualização Anexo','Ano Rollout','Data Fechamento Ordem Complexa','Elemento ID','Endereço ID Site Atendido','Hora Atualização Ordem Complexa','Hora Criação Ordem Complexa','Log Activity','Usuário Criação Ordem Complexa','Usuário de Alteração Ordem Complexa','Caminho','Date Load']
    fields2 = ['ANF','OrdemComplexa','Nome','Tipo','DataCriacao','UsuarioCriacao','DataAtualizacao','UserAtualizacaoAnexo','AnoRollout','DataFechamentoOrdem','ElementoID','EnderecoID','HoraAtualizacaoOrdem','HoraCriacaoOrdem','LogActivity','UserCriacaoOrdem','UseerAlteracaoOrdem','Caminho','DateLoad']
    
    pathImport = '/import/R041'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/R041/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.xlsx")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        #data = pd.read_excel(filename,skiprows=27,sheet_name = 'DUMP_5G_DSS', nrows=40,usecols = 'A:AC')
        data = pd.read_excel(filename,sheet_name = 'Sheet1',usecols = fields)
        df = df.append(data,ignore_index=True)
        df = df[fields] # ordering labels
        li.append(df)  
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.loc[frameSI['ANF'].astype(int).isin([11,12,13,14,15,16,17,18,19])]

    
    DataList = ['DataCriacao','DataAtualizacao','DataFechamentoOrdem','HoraAtualizacaoOrdem','HoraCriacaoOrdem','DateLoad']
    for i in DataList:
      frameSI[i] = pd.to_datetime(frameSI[i], format="%d/%m/%Y hh:mm:ss")

    frameSI.loc[frameSI['Tipo'].str.contains('TSSR|TSSr RF|RFSheet|CDD|Initial Tunning')==True,['Keep']] = 'Keep'
    #df[df['value'].astype(str).str.contains('1234.+')] for filtering out non-string-type columns.
    #df[df['A'].str.contains("Hello|Britain")==True]

    frameSI = frameSI.loc[frameSI['Keep'] == 'Keep']
    frameSI.sort_values(['OrdemComplexa','Tipo','DataAtualizacao'], ascending = [True,True,False],inplace=True)
    subnetcheck = ['OrdemComplexa','Tipo']
    frameSI.drop_duplicates(subset=subnetcheck, keep='first', inplace=True, ignore_index=False)

    frameSI.loc[frameSI['Tipo'].str.contains('TSSR|TSSr RF')==True,['TSSR Data']] = frameSI['DataAtualizacao'].dt.strftime('%d/%m/%Y')
    frameSI.loc[frameSI['Tipo'].str.contains('RFSheet')==True,['RF SHEET Data']] = frameSI['DataAtualizacao'].dt.strftime('%d/%m/%Y')
    frameSI.loc[frameSI['Tipo'].str.contains('CDD')==True,['CDD Data']] = frameSI['DataAtualizacao'].dt.strftime('%d/%m/%Y')
    frameSI.loc[frameSI['Tipo'].str.contains('Initial Tunning')==True,['INITIAL TUNNING Data']] = frameSI['DataAtualizacao'].dt.strftime('%d/%m/%Y')
    
    #DataList = ['TSSR Data','RF SHEET Data','CDD Data','INITIAL TUNNING Data']
  



    '''
    
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
      '''
    print(frameSI)
    



    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')


