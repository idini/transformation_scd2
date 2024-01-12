# Worldline - Data Engineer Transformation SCD2 use case

**Maurizio Idini**
19/01/2024

## Introduction

This is a simple document that briefly describes the repository.

The repository contains two main folders:
- `sql` folder, that contains SQL code to create and populate tables in BigQuery
- `transformation` that contains Python code that perform table transformations based on *Slowly Changing Dimension type2*

The module is written in **Python 3.8**, using **Docker** container.
The lib is written using **google-cloud** library, documented using **Docstring** and tested using **pytest**.

*Discaimer*
The code is written for the transformation module, it is assumed that the bigquery tables already exists.

## Project Description

The folder structure is
```markdown
worldline_bq_usecase
 ┣ bin
 ┃ ┣ down.sh
 ┃ ┣ exec.sh
 ┃ ┣ test.sh
 ┃ ┣ test.up.sh
 ┃ ┗ up.sh
 ┣ lib
 ┃ ┣ transformation
 ┃ ┃ ┣ transformator.py
 ┃ ┃ ┗ tbd.py
 ┃ ┣ tbd
 ┃ ┃ ┗ tbd.py
 ┃ ┣ dataencryption
 ┃ ┗ storage
 ┃ ┃ ┣ BigqueryStorageManager.py
 ┃ ┃ ┣ tbd.py
 ┃ ┃ ┣ tbd_test.py
 ┃ ┃ ┗ StorageManager.py
 ┣ Dockerfile
 ┣ app.py
 ┣ docker-compose.yml
 ┗ requirements.txt
```

The main folder contains

 - `requirements.txt` with the libraries used in the project
 - `Dockerfile` and `docker-compose.yml` for the Docker container
 - `lib` that contains the code and unit tests
 - `bin` folder with bash script useful to run docker environments
 - `app.py` that contains code TBD

The `lib` code is composed by

 - `tbd` code that perform tbd

The `storage` folder contains `BigqueryManager` to perform read/write operations on Google BigQuery.

## Run the code

You can run the code in two ways:
 -  using `python app.py`
 -  using Docker, running `./bin/up.sh`

 You can also access to `test_env` Docker container in two ways, running
 - `./bin/test.up.sh`
 - `./bin/up.sh` and `./bin/exec.sh test_env bash`

