# main_server/Dockerfile
FROM base_server:latest

WORKDIR /app

COPY requirements.txt .
COPY . .

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main_server:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
