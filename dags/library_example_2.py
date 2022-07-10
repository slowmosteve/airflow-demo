from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json

@dag(
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime.today(),
    catchup=False,
    tags=['example']
)
def openlibrary_2():
    """
    ### DAG defined using Airflow 2.0 TaskFlow API to simplify the pipeline definition
    """

    @task
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
            return subject_works
        else:
            raise ValueError('Failed to retrieve works')

    @task(multiple_outputs=True)
    def load_to_file(input_filename, subject_works):
        """Load response data to a text file with newline delimited JSON format
        """
        print(f'subject_works: {subject_works}')
        print(f'input_filename: {input_filename}')
        file = open(input_filename, 'w')
        for work in subject_works:
            file.write(json.dumps(work) + '\n')
        file.close
        return {'input_filename': input_filename, 'subject_works_count': len(subject_works)}

    @task
    def transform_file_data(input_filename):
        """Parse text file for select fields and write to new JSON file
        """
        with open(input_filename, 'r') as in_file:
            books = []
            for line in in_file:
                line_json = json.loads(line)
                books.append({'title': line_json.get('title'), 'authors' : line_json.get('authors')})
        output_filename = 'library_output_2.json'
        with open(output_filename, 'w') as out_file:
            json.dump(books, out_file)

    input_filename = 'library_input_2.txt'
    subject_works = get_subject_works(subject_keyword='time')
    load_to_file_task_instance = load_to_file(input_filename, subject_works)
    transform_file_data(load_to_file_task_instance['input_filename'])

library_2_dag = openlibrary_2()