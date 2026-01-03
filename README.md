# monitoring

Небольшой проект для локального стенда логирования на базе **Grafana + Loki**.

Репозиторий: [`RomanShe19/monitoring`](https://github.com/RomanShe19/monitoring)

## Структура

- `main.py` — генерация и отправка тестовых логов в Loki (чтобы было что визуализировать в Grafana)
- `Loki+Grafana/` — docker-compose стенд:
  - `docker-compose.yml` — поднимает `loki` и `grafana` в одной сети, Grafana ходит в Loki по имени сервиса `loki`
  - `loki-config.yml` — конфигурация Loki
  - `grafana/provisioning/datasources/loki.yml` — автоподключение Loki datasource в Grafana

## Быстрый старт

```bash
cd Loki+Grafana
docker compose up -d
```

## Генерация тестовых логов

После запуска compose можно отправлять тестовые логи:

```bash
python3 main.py --loki-url http://localhost:3100 --count 50 --interval 0.2 --label env=dev
```

## Доступы

- **Grafana**: `http://localhost:3000`
  - user: `admin`
  - password: `admin`
- **Loki**: `http://localhost:3100`

## Примечания

- **Datasource Loki в Grafana создаётся автоматически** через provisioning и указывает на `http://loki:3100`.


