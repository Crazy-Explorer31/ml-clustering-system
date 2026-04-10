# main_server/Dockerfile
FROM python:3.11

WORKDIR /app

COPY requirements.txt .
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "main_server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
