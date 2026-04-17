# jobs_server/Dockerfile
FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# RUN wget https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc.en.300.bin.gz
# RUN gzip -d cc.en.300.bin.gz
COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "jobs_server:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]
