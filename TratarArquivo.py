import ImportDF
import numpy as np
import pandas as pd


Exp_fields = ['ColumnNAME','If_is_IN','OBSColumn','OBSValue','REGRA_EDB','REGRA_TIM']
Exp_pathImport = '/import/Filter'
Exp = ImportDF.ImportDF(Exp_fields,Exp_pathImport)

Excep_fields = ['ORDEM','Analise_VENDOR']
Excep_pathImport = '/import/EXCECAO'
Excep = ImportDF.ImportDF(Excep_fields,Excep_pathImport)



def processArchive(frameSI):
    for index, row in Exp.iterrows():
        try:
          frameSI.loc[(frameSI[row['ColumnNAME']].str.contains(row['If_is_IN'])),[row['OBSColumn']]] = row['OBSValue']
          frameSI.loc[(frameSI[row['ColumnNAME']].str.contains(row['If_is_IN'])),['REGRA_EDB']] = row['REGRA_EDB']
          frameSI.loc[(frameSI[row['ColumnNAME']].str.contains(row['If_is_IN'])),['REGRA_TIM']] = row['REGRA_TIM']
          frameSI.loc[frameSI[row['OBSColumn']].isna(),[row['OBSColumn']]] = 'OUTDOOR'
          frameSI.loc[frameSI['REGRA_EDB'].isna(),['REGRA_EDB']] = 'TSSR|RF SHEET|CDD|INITIAL TUNNING'
          frameSI.loc[frameSI['REGRA_TIM'].isna(),['REGRA_TIM']] = ''
          
        except:
           pass 
    
    frameSI = compare(frameSI,'Pending_Licceu','REGRA_EDB')
    frameSI = compare(frameSI,'Pending_Licceu','REGRA_TIM')

    frameSI = pd.merge(frameSI,Excep, how='left',left_on=['ORDEM_COMPLEXA_PMO'],right_on=['ORDEM'])
    frameSI.drop(['ORDEM'],axis=1,inplace = True)
    

    frameSI['REAL_ACEITACAO_LOGICA_PMO'] = frameSI['REAL_ACEITACAO_LOGICA_PMO'].replace('N/A', '-')
    frameSI['REAL_ACEITACAO_LOGICA_PMO2'] = pd.to_datetime(frameSI['REAL_ACEITACAO_LOGICA_PMO'], format='%d/%m/%Y', errors='coerce')
    frameSI['DateArchive_PMO2'] = pd.to_datetime(frameSI['DateArchive_PMO'], format='%Y%m%d', errors='coerce')
    frameSI['DaysAfterREAL_ACEITACAO'] = (frameSI['DateArchive_PMO2'] - frameSI['REAL_ACEITACAO_LOGICA_PMO2']).dt.days

    #frameSI.loc[frameSI['REAL_ACEITACAO_LOGICA_PMO']=='-',['REAL_ACEITACAO_LOGICA_PMO']] = ''
    #frameSI['REAL_ACEITACAO_LOGICA_PMO2'] = pd.to_datetime(frameSI['REAL_ACEITACAO_LOGICA_PMO'], format="%d/%m/%Y")
    #frameSI.loc[frameSI['REAL_ACEITACAO_LOGICA_PMO']!='-',['REAL_ACEITACAO_LOGICA_PMO2']] = pd.to_datetime(frameSI.loc[frameSI['REAL_ACEITACAO_LOGICA_PMO']!='-'], format="%d/%m/%Y")
    #frameSI.loc[frameSI['DateArchive_PMO']!='-',['DateArchive_PMO2']] = pd.to_datetime(frameSI['DateArchive_PMO'], format="%Y%m%d")
    #frameSI.loc[frameSI['REAL_ACEITACAO_LOGICA_PMO']!='-',['DaysAfterREAL_ACEITACAO']] = (frameSI['DateArchive_PMO2'] - frameSI['REAL_ACEITACAO_LOGICA_PMO2']).dt.days
    
    #frameSI['DateArchive_PMO2'] = pd.to_datetime(frameSI['DateArchive_PMO'], format="%Y%m%d")
    #frameSI['DaysAfterREAL_ACEITACAO'] = (frameSI['DateArchive_PMO2'] - frameSI['REAL_ACEITACAO_LOGICA_PMO2']).dt.days
         
           
    #frameSI['DateArchive_ref'] = frameSI['PLANO_PMO'] + '_' +frameSI['DateArchive_Licceu']
    #frameSI['DateArchive_ref'] = frameSI['DateArchive_PMO2'].dt.strftime('%YW%U')
    frameSI['DateArchive_ref'] = frameSI['PLANO_PMO'] + frameSI['DateArchive_PMO2'].dt.strftime('W%U')
    frameSI['DateArchive_Export'] = frameSI['DateArchive_PMO2'].dt.strftime('%YW%U')

    #print(frameSI[['STATUS_Licceu','Pending_Licceu','CLASSIFICACAO','REGRA_EDB']])       

    return frameSI



def compare(frameSI,columnA, columnB):
   for index, row in frameSI.iterrows():
       vector1 = row[columnA].split('|')
       vector2 = row[columnB].split('|')
       result = np.intersect1d(vector1, vector2)
       #result_inverse = np.setdiff1d(vector1, vector2)
       string = '|'.join(str(x) for x in result)
       if result.size > 0:
        frameSI.at[index,['STATUS_'+columnB[-3:]]] = 'NOT OK'
        frameSI.at[index,['STATUS_'+columnB[-3:]+'_PEndencia']] = string
        
       else:
        frameSI.at[index,['STATUS_'+columnB[-3:]]] = 'OK'
        frameSI.at[index,['STATUS_'+columnB[-3:]+'_PEndencia']] = 'OK' 
   return frameSI








