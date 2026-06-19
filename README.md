# ML-система кластеризации

* Система доступна на http://108.165.32.182/ui (креды для тестирования: `user` `user`)

## Демонстрация работы

[demo](https://drive.google.com/file/d/1NKPY-a0EYWySJu2vguC0hslDr2bCaKY8/view?usp=drive_link)

## Запуск системы

* Создание базового образа
```
docker build -t base_server:latest -f base_server/Dockerfile .
```
* Запуск
* * На удаленной машине
```bash
docker-compose -f docker-compose-production.yml up --build
```
* * На локальной машине
```bash
docker-compose up --build
```
* * Для гарантированной остановки сервиса выполнить
```bash
docker-compose down --remove-orphans
```
* При первом запуске создайте в S3 хранилище (обычно на `http://<your_ip>:9001`) два бакета: `datasets` и `results`.

