# ML-система кластеризации

* Сервер доступен на http://108.165.32.182/ui

## Порядок действия для поднятия сервера

* Создание базового образа
```
docker build -t base_server:latest -f base_server/Dockerfile .
```
* Запуск
```
docker-compose down --remove-orphans && docker-compose -f docker-compose-production.yml up --build
```
* При первом запуске перейдите по `http://127.0.0.1:9001` и создайте в хранилище два бакета: `datasets` и `results`.
![Alt Text](docs/record.gif)
