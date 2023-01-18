import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import ImportDF

def processArchive():
  print("\nProcessing " + os.path.basename(__file__) + '...')
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
  csv_path = os.path.join(script_dir, 'export/'+'MERGE'+'/'+ 'Merged' +'.csv')
 

  PMO_fields = ['PLANO','ORDEM_COMPLEXA','STATUS_OC','ELEMENTO_ID','FREQUENCIA_ROLLOUT','ENDERECO_ID','CLASSE_SITE','CLASSIFICACAO_CASA','TECNOLOGIA','REGIONAL','UF','ANF','COD_IBGE','TIPO_SITE','ETAPA_ATUAL','TIPO_ELEMENTO','STATUS_FINANCEIRO','#BBU','REAL_ATIVACAO_NETFLOW']
  PMO_pathImport = '/export/PMO'

  PMO = ImportDF.ImportDF(PMO_fields,PMO_pathImport)
  PMO.name = 'PMO'
  PMO = change_columnsName(PMO)


  Licceu_fields = ['OC_NetFlow','STATUS','Pending']
  Licceu_pathImport = '/export/Licceu'

  Licceu = ImportDF.ImportDF(Licceu_fields,Licceu_pathImport)
  Licceu.name = 'Licceu'
  Licceu = change_columnsName(Licceu)

  Merged = pd.merge(PMO,Licceu, how='left',left_on=['ORDEM_COMPLEXA_PMO'],right_on=['OC_NetFlow_Licceu'])
  Merged.to_csv(csv_path,index=True,header=True,sep=';')


    


















def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df    