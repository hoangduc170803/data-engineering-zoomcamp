FROM python:3.9.21

RUN pip install pandas sqlalchemy psycopg2
RUN apt-get install curl
WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]