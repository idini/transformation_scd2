# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /transformation_scd2

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

ENV GOOGLE_APPLICATION_CREDENTIALS=/transformation_scd2/credentials/application_default_credentials.json

CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]

