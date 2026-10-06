FROM python:3.12-slim

COPY ./requirements.txt .
RUN pip install --no-cache-dir --require-hashes -r requirements.txt


