# fastapi_app/Dockerfile
FROM python:3.9

WORKDIR /app

COPY requirements.txt .
COPY umath-validation-set-log.csv .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main_server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
