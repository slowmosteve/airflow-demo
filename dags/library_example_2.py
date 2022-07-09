from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

@dag(
    schedule_interval=timedelta(days=1),
    start_date=datetime.today(),
    catchup=False,
    tags=['example']
)
def openlibrary_2():
    """
    ### DAG defined using Airflow 2.0 API to simplify the pipeline definition
    """

    @task
    def get_subject_works(subject_keyword):
        """Retrieve's subject works from openlibrary API
        """
        url_base = 'https://openlibrary.org'
        url_path_authors = f'/subjects/{subject_keyword}.json'
        query_params_authors = {'limit': 10}
        subject_response = requests.get(url=(url_base + url_path_authors), params=query_params_authors)
        print(f'Request status: {subject_response.status_code}')

        if subject_response.status_code == 200:
            subject_works = subject_response.json()['works']
            for work in subject_works:
                print('Title: {}'.format(work['title']))
            return 200, subject_works
        else:
            raise ValueError('Failed to retrieve works')

    get_subject_works(subject_keyword='time')

library_2_dag = openlibrary_2()