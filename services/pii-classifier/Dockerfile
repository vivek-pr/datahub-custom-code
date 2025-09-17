FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY services/pii-classifier/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY services/pii-classifier/pii_classifier ./pii_classifier
COPY sample/regex ./sample/regex

ENTRYPOINT ["python", "-m", "pii_classifier.cli"]
