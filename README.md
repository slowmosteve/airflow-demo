# Airflow demo

## Installation

Set up virtual environment

```
python -m venv .airflow-venv
source .airflow-venv/bin/activate
```

[Install airflow from PyPi](https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html)

Pay attention to the constraints which ensure the Airflow dependencies work with the version of application.

```
AIRFLOW_VERSION=2.3.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow[google]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```

Once all libraries have been installed, freeze them to a `requirements.txt` file

```
pip freeze > requirements.txt
```

## Airflow setup and initialization

Create a separate directory for airflow and set `AIRFLOW_HOME` to reference it
```
mkdir airflow
export AIRFLOW_HOME=$(pwd)/airflow
```

Update the `airflow.cfg` file:
- set `load_examples = False` so that the example DAGS aren't loaded.
- set `dags_folder` to point to the `/dags` directory (e.g. `dags_folder=/Users/me/projects/airflow-demo/dags`)


## Run Airflow

Use the `standalone` command to initialize the database, make a user and start all components for you. Note that `standalone` should not be used for production deployments and is used for simplicity in this demo. Read more [here](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html).
```
airflow standalone
```

Open `http://localhost:8080/` in a browser to access the web UI. Note that the initial admin password will be created and saved in the `/airflow/standalone_admin_password.txt` file. You can change the password in the web UI and then delete this file.


## DAG examples

`library_example_1.py` shows the conventional way of authoring DAGs from Airflow 1.0

`library_example_2.py` shows the same DAG using the TaskFlow API from Airflow 2.0


## TO DO
- update `library_example_2.py` to recreate the same steps as `library_example_1.py`