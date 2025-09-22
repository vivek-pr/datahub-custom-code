FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN adduser --disabled-password --gecos "" appuser
WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY action action

USER appuser

EXPOSE 8080

CMD ["uvicorn", "action.app:create_app", "--host", "0.0.0.0", "--port", "8080"]
