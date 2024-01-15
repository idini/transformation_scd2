# Worldline - Data Engineer Transformation SCD2 use case

**Maurizio Idini**

[maurizio.idini@gmail.com](mailto:maurizio.idini@gmail.com)
16/01/2024

## Introduction

This is a simple document that briefly describes the repository.
The aim of the use case implemented in this repository is to perform table transformations based on [*Slowly Changing Dimension type2*](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row)

The module is written in **Python 3.8**, using **Docker** container.
The lib is written using **google-cloud** library, documented using **Docstring** and tested using **pytest**.

#### Disclaimer

The code is written for the transformation module, it is assumed that the bigquery tables already exists.

Before run the code, please check if the `application_default_credential.json` credential file is already in your machine, in the path `~/.config/gloud/`.
If not, please run the following command from terminal `gcloud auth application-default login` and follow the steps.

## Project Description

The folder structure is detailed in the following section:
<details>
  <summary>Show project structure</summary>
  
    worldline_bq_usecase
    ┣ bin
    ┃ ┣ down.sh
    ┃ ┣ test.sh
    ┃ ┣ test.up.sh
    ┃ ┗ up.sh
    ┣ lib
    ┃ ┣ data
    ┃ ┃ ┣ comparer
    ┃ ┃ ┃ ┣ TableComparer.py
    ┃ ┃ ┃ ┗ __init__.py
    ┃ ┃ ┣ ingestor
    ┃ ┃ ┃ ┣ DataIngestor.py
    ┃ ┃ ┃ ┗ __init__.py
    ┃ ┃ ┗ __init__.py
    ┃ ┗ dbmanagement
    ┃ ┃ ┣ connector
    ┃ ┃ ┃ ┣ BigQueryConnector.py
    ┃ ┃ ┃ ┗ __init__.py
    ┃ ┃ ┣ tablemanagement
    ┃ ┃ ┃ ┣ BigQueryManager.py
    ┃ ┃ ┃ ┗ __init__.py
    ┃ ┃ ┣ transaction
    ┃ ┃ ┃ ┣ BigquerySession.py
    ┃ ┃ ┃ ┣ BigqueryTransaction.py
    ┃ ┃ ┃ ┗ __init__.py
    ┃ ┃ ┗ __init__.py
    ┣ sql_example
    ┃ ┣ setup_tables
    ┃ ┃ ┣ create_populate_Table2_Partners_Output.sql
    ┃ ┃ ┗ create_populate_Table_1_Partners_Input.sql
    ┃ ┣ simulate_update_source.sql
    ┃ ┗ update_table_2_partners_output.sql
    ┣ tests
    ┃ ┣ integration
    ┃ ┃ ┣ data
    ┃ ┃ ┃ ┣ comparer
    ┃ ┃ ┃ ┃ ┗ TableComparer_test.py
    ┃ ┃ ┃ ┗ ingestor
    ┃ ┃ ┃ ┃ ┗ DataIngestor_test.py
    ┃ ┃ ┗ dbmanagement
    ┃ ┃ ┃ ┣ connector
    ┃ ┃ ┃ ┃ ┗ BigQueryConnector_test.py
    ┃ ┃ ┃ ┗ tablemanagement
    ┃ ┃ ┃ ┃ ┗ BigQueryManager_test.py
    ┃ ┗ unit
    ┃ ┃ ┗ data
    ┃ ┃ ┃ ┣ comparer
    ┃ ┃ ┃ ┃ ┗ TableComparer_test.py
    ┃ ┃ ┃ ┗ ingestor
    ┃ ┃ ┃ ┃ ┗ DataIngestor_test.py
    ┣ .gitignore
    ┣ Dockerfile
    ┣ README.md
    ┣ app.py
    ┣ docker-compose.yml
    ┗ requirements.txt

    
</details>


The main folder contains

 - `requirements.txt` with the libraries used in the project
 - `Dockerfile` and `docker-compose.yml` for the Docker container
 - `lib` that contains the code
 - `bin` folder with bash script useful to run docker environments
 - `app.py` that contains code FlaskAPI code to trigger the process
 - `README.md` that contains this description

The `lib` code is composed by

 - `data` folder that contains code for *comparer* and *ingestor*
 - `dbmanagement` folder that contains code for *connector*, **tablemanagement* and *transaction*

Furthermore, the repository contains a `sql_example` folder with sql files to
 - [generate tables](./sql_example/setup_tables)
 - [simulate transformation tables](./sql_example/update_table_2_partners_output.sql2)

## Run the code

You can run the code in two ways:
 -  using `python app.py` in your local machine
 -  using Docker, running `./bin/up.sh`

 You can run the tests accessing to `test_transformation_scd2` Docker container running
 - `./bin/test.up.sh` and `./bin/test.sh`
 - `./bin/test.sh` in your local machine

Verify the deployment by navigating to your server address in your preferred browser.

```sh
127.0.0.1:5001
```

## Future improvements
 -  Check schema between source and destination table
 -  Implement *Factory pattern* in `Connector`, `TableManagement` and `Transaction` in order to extend DataIngestor with the use of others DB
