FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \ 
    && apt-get install -y --no-install-recommends build-essential libpq-dev \ 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY action action

RUN useradd --uid 10001 --home-dir /home/appuser --create-home appuser \ 
    && chown -R 10001:10001 /app

USER 10001

EXPOSE 8080

CMD ["uvicorn", "action.app:app", "--host", "0.0.0.0", "--port", "8080"]
