# Loki + Grafana (docker-compose)

## Доступы

- **Grafana**: `http://localhost:3000`
  - логин: `admin`
  - пароль: `admin`
- **Loki**: `http://localhost:3100`

## Запуск

Из папки `Loki+Grafana`:

```bash
docker compose up -d
```

## Отправка тестовых логов в Loki (Python)

В репозитории есть скрипт `send_test_logs.py`. Это **не установленная команда**, а файл, который нужно запустить.

### Вариант 1: запуск через python3

```bash
python3 send_test_logs.py --loki-url http://localhost:3100 --message "hello loki"
```

### Вариант 2: сделать исполняемым и запускать как команду

```bash
chmod +x send_test_logs.py
./send_test_logs.py --loki-url http://localhost:3100 --count 10 --interval 0.2 --label app=demo --message "test"
```

### Если запускаешь скрипт на другом устройстве

Укажи адрес сервера, где доступен Loki (порт `3100`):

```bash
python3 send_test_logs.py --loki-url http://<SERVER_IP_OR_DNS>:3100 --message "from my laptop"
```

Важно: порт `3100` должен быть доступен по сети (firewall / security group / проброс порта).

### Быстрая проверка, что Loki доступен

```bash
curl -sS http://localhost:3100/ready && echo
```


