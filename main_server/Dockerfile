# main_server/Dockerfile
FROM base_server:latest

WORKDIR /app

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main_server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
