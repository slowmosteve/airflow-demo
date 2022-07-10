from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json


def get_subject_works(subject_keyword):
    """Retrieve's subject works from openlibrary API
    """
    url_base = 'https://openlibrary.org'
    url_path = f'/subjects/{subject_keyword}.json'
    query_params = {'limit': 10}
    subject_response = requests.get(url=(url_base + url_path), params=query_params)
    print(f'Request status: {subject_response.status_code}')

    if subject_response.status_code == 200:
        subject_works = subject_response.json()['works']
        for work in subject_works:
            print('Title: {}'.format(work['title']))
        return {'subject_works': subject_works}
    else:
        raise ValueError('Failed to retrieve works')

def load_to_file(**kwargs):
    """Load response data to a text file with newline delimited JSON format
    """
    subject_works = kwargs['ti'].xcom_pull(task_ids='get_subject_works')['subject_works']
    print(f'subject_works: {subject_works}')
    input_filename = 'library_input_1.txt'
    file = open(input_filename, 'w')
    for work in subject_works:
        file.write(json.dumps(work) + '\n')
    file.close
     
def transform_file_data(**kwargs):
    """Parse text file for select fields and write to new JSON file
    """
    input_filename = kwargs['input_file']
    with open(input_filename, 'r') as in_file:
        books = []
        for line in in_file:
            line_json = json.loads(line)
            books.append({'title': line_json.get('title'), 'authors' : line_json.get('authors')})
    output_filename = 'library_output_1.json'
    with open(output_filename, 'w') as out_file:
        json.dump(books, out_file)

with DAG(
    'openlibrary_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    description='An example DAG defined using legacy Airflow 1.0 conventions (i.e. without TaskFlow API)',
    schedule_interval=timedelta(days=1),
    start_date=datetime.today(),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='get_subject_works',
        python_callable=get_subject_works,
        provide_context=True,
        op_kwargs={
            'subject_keyword': 'time'
        }
    )
    t2 = PythonOperator(
        task_id='load_to_file',
        python_callable=load_to_file,
        provide_context=True
    )
    t3 = PythonOperator(
        task_id='transform_file_data',
        python_callable=transform_file_data,
        op_kwargs={
            'input_file': 'library_input_1.txt'
        }
    )

    t1 >> t2 >> t3