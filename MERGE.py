import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import ImportDF
import TratarArquivo
import unique
import Count

def processArchive():
  print("\nProcessing " + os.path.basename(__file__) + '...')
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'/'+ 'Merged' +'.csv')
 

  PMO_fields = ['PLANO','NOMEPLANO','PROJETO','PROJETO_FINANCEIRO','FORNECEDOR','OBS','ORDEM_COMPLEXA','STATUS_OC','ELEMENTO_ID','FREQUENCIA_ROLLOUT','ENDERECO_ID','CLASSE_SITE','CLASSIFICACAO_CASA','TECNOLOGIA','REGIONAL','UF','ANF','COD_IBGE','TIPO_SITE','ETAPA_ATUAL','TIPO_ELEMENTO','STATUS_FINANCEIRO','REAL_ATIVACAO_NETFLOW','REAL_ACEITACAO_LOGICA','DateArchive']
  PMO_pathImport = '/export/PMO'

  PMO = ImportDF.ImportDF(PMO_fields,PMO_pathImport)
  PMO.name = 'PMO'
  PMO = change_columnsName(PMO)


  Licceu_fields = ['OC_NetFlow','STATUS','Pending','DateArchive']
  Licceu_pathImport = '/export/Licceu'

  Licceu = ImportDF.ImportDF(Licceu_fields,Licceu_pathImport)
  Licceu.name = 'Licceu'
  Licceu = change_columnsName(Licceu)

  Merged = pd.merge(PMO,Licceu, how='left',left_on=['ORDEM_COMPLEXA_PMO'],right_on=['OC_NetFlow_Licceu'])
  Merged.drop(['OC_NetFlow_Licceu'],1,inplace = True)
  Merged.loc[Merged['STATUS_Licceu'].isna(),['STATUS_Licceu']] = 'Verificar OC'

  #Status COVERAGE INDOOR como tratar?
  Merged = Merged.loc[Merged['STATUS_Licceu'] != 'Verificar OC']

  Merged = TratarArquivo.processArchive(Merged)

  
  
  #Removing not action
  Merged = Merged.loc[(Merged['STATUS_TIM'].astype(str) != 'OK')|(Merged['STATUS_EDB'].astype(str) != 'OK')]
  Merged2 = Merged.loc[(Merged['STATUS_OC_PMO'].str.contains('REAL_ATIVACAO_NETFLOW|CLOSED')) &((Merged['STATUS_TIM'].astype(str) != 'OK')|(Merged['STATUS_EDB'].astype(str) != 'OK'))]
  
  Merged2['RefCountEDB'] = Merged2['DateArchive_ref'] +  Merged2['STATUS_EDB']
  Merged2['RefCountTIM'] = Merged2['DateArchive_ref'] +  Merged2['STATUS_TIM']

  CountArquivoEDB = Count.count(Merged2,'RefCountEDB')
  CountArquivoEDB.rename(columns={'count': 'count_EDB'}, inplace=True)
  CountArquivoTIM = Count.count(Merged2,'RefCountTIM')
  CountArquivoTIM.rename(columns={'count': 'count_TIM'}, inplace=True)

  Merged2 = pd.merge(Merged2,CountArquivoEDB, how='left',left_on=['RefCountEDB'],right_on=['RefCountEDB'])
  Merged2 = pd.merge(Merged2,CountArquivoTIM, how='left',left_on=['RefCountTIM'],right_on=['RefCountTIM'])

  

  Merged.to_csv(csv_path,index=False,header=True,sep=';')
  csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'/'+ 'Merged_Filtered' +'.csv')
  #droplist 'RefCountEDB','RefCountTIM','count_EDB','count_TIM'
  Merged2.to_csv(csv_path,index=False,header=True,sep=';')

  Merged3 = Merged2.copy()
  KeepList2 = ['PLANO_PMO', 'DateArchive_ref','DateArchive_Export','STATUS_EDB','STATUS_TIM','count_EDB','count_TIM']
  locationBase_top = list(Merged3.columns)
  res = list(set(locationBase_top)^set(KeepList2))
  Merged3 = Merged3.drop(res,axis=1)
  Merged3.drop_duplicates(inplace=True)
  csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'/'+ 'Merged_Filtered_Consolidado' +'.csv')
  Merged3.to_csv(csv_path,index=False,header=True,sep=';')

  



  

    


















def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df    