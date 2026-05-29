# jobs_server/Dockerfile
# FROM python:3.11
FROM base_server:latest

WORKDIR /app

COPY requirements.txt .
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "jobs_server:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
