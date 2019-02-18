from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 
from pandas.io.json import json_normalize
from pathlib import Path

p = Path('.').cwd()
raw_data = p/'raw_data'
treated_data = p/'treated_data'

default_args = {
    'owner': 'brow1998',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG('clean', default_args=default_args, schedule_interval=timedelta(days=1))

def clean_infomix_data():
    infomix = pd.read_csv(raw_data/'infomix.tsv', sep='\t')

    infomix['category'] = infomix.category.str.upper()

    infomix.to_csv(treated_data/'infomix_treated.csv', index=False)

def clean_gs1_data():
    gs1 = pd.read_json(raw_data/'gs1.jl', lines=True)
    gs1_responses = pd.io.json.json_normalize(gs1.response)
    gtins = pd.merge(gs1, gs1_responses, left_index=True, right_index=True)    
    
    gtins = gtins.rename(columns={'cnpj_manufacturer':'cnpj',
                                  'gepirParty.partyDataLine.address.city':'city',
                                  'gepirParty.partyDataLine.address.state':'state'})
    
    gtins = gtins[(gtins['status'] == 'OK') & (gtins['cnpj'].notnull())]

    gtins[['cnpj','gtin', 'city', 'state']].to_csv(treated_data/'gs1_treated.csv', index=False)

def clean_cosmos_data():
    cosmos = pd.read_json(raw_data/'cosmos.jl', lines=True)
    cosmos_responses = pd.io.json.json_normalize(cosmos.response)
    cosmos_full = pd.merge(cosmos, cosmos_responses, left_index=True, right_index=True) 

    cosmos_full = cosmos_full[cosmos_full['status'] == 'OK']
    cosmos_full = cosmos_full[['gtin_y','description']].rename(columns={'gtin_y':'gtin'})
    
    cosmos_full.to_csv(treated_data/'cosmos_treated.csv',index=False)

def clean_cnpjs_data():
    cnpjs = pd.read_json(raw_data/'cnpjs_receita_federal.jl', lines=True)
    cnpjs_responses = pd.io.json.json_normalize(cnpjs.response)
    cnpjs_full = pd.merge(cnpjs, cnpjs_responses, left_index=True, right_index=True) 
    
    cnpjs_full = cnpjs_full[cnpjs_full['situacao'] == 'ATIVA']
    cnpjs_full = cnpjs_full[['nome','cnpj_x','municipio','uf']].rename(columns={'cnpj_x':'cnpj', 'uf':'state', 'municipio':'city'})

    cnpjs_full.to_csv(treated_data/'cnpjs_treated.csv', index=False)

def validate_gtins():
    gs1 = pd.read_csv(treated_data/'gs1_treated.csv')
    cnpjs = pd.read_csv(treated_data/'cnpjs_treated.csv')

    combined_data = pd.merge(left=gs1, right=cnpjs, how='left', on=['cnpj','state','city'])
    combined_data = combined_data[combined_data['name'].notnull()]

    combined_data.to_csv(treated_data/'valid_gtins.csv', index=False)

def enrich_gtins():
    cosmos = pd.read_csv(treated_data/'cosmos_treated.csv')
    valid_gtins = pd.read_csv(treated_data/'valid_gtins.csv')
    
    gtin_data = pd.merge(left=valid_gtins, right=cosmos, on='gtin', how='left')
    gtin_data = gtin_data[gtin_data['description'].notnull()]

    gtin_data.to_csv(treated_data/'gtin_data.csv', index=False)

def export_final_result():
    infomix = pd.read_csv(treated_data/'infomix_treated.csv')
    gtin_data = pd.read_csv(treated_data/'gtin_data.csv')

    final_df = pd.merge(infomix, gtin_data, how='inner',on='gtin')
    final_df = final_df.rename(columns={'cnpj_x':'cnpj_store', 'cnpj_y':'cnpj_manufacturer'})

    final_df.to_csv(treated_data/'final_result.csv', index=False)

t1 = PythonOperator(
    task_id='clean_infomix',
    python_callable=clean_infomix_data,
    dag=dag)

t2 = PythonOperator(
    task_id='clean_cosmos',
    python_callable=clean_cosmos_data,
    dag=dag)

t3 = PythonOperator(
task_id='clean_cnpjs',
python_callable=clean_cnpjs_data,
dag=dag)

t4 = PythonOperator(
task_id='clean_gs1',
python_callable=clean_gs1_data,
dag=dag)

t5 = PythonOperator(
    task_id='validate_gtins',
    python_callable=validate_gtins,
    dag=dag)

t6 = PythonOperator(
    task_id='enrich_gtins',
    python_callable=enrich_gtins,
    dag=dag)

t7 = PythonOperator(
task_id='load_into_csv',
python_callable=export_final_result,
dag=dag)




t1 << t2 << t3 << t4 << t5 << t6 << t7