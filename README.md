# Команды управления RabbitMQ

## 1. Общее управление

### Показать статус RabbitMQ
```bash
rabbitmqctl status
```

### Вывести список всех узлов в кластере RabbitMQ
```bash
rabbitmqctl cluster_status
```

### Остановить приложение RabbitMQ
```bash
rabbitmqctl stop_app
```

### Запустить приложение RabbitMQ
```bash
rabbitmqctl start_app
```

### Сбросить узел RabbitMQ (удаление всех данных)
```bash
rabbitmqctl reset
```

### Принудительный сброс узла RabbitMQ (в случае несогласованности)
```bash
rabbitmqctl force_reset
```

### Список включенных плагинов RabbitMQ
```bash
rabbitmq-plugins list --enabled
```

### Включить плагин RabbitMQ
```bash
rabbitmq-plugins enable <plugin_name>
```

### Отключить плагин RabbitMQ
```bash
rabbitmq-plugins disable <plugin_name>
```

## 2. Управление очередями

### Список всех очередей
```bash
rabbitmqctl list_queues
```

### Список очередей с дополнительными деталями
```bash
rabbitmqctl list_queues name messages_ready messages_unacknowledged consumers
```

### Удалить конкретную очередь
```bash
rabbitmqctl delete_queue <queue_name>
```

### Очистить конкретную очередь
```bash
rabbitmqctl purge_queue <queue_name>
```

## 3. Управление обменниками

### Список всех обменников
```bash
rabbitmqctl list_exchanges
```

### Удалить обменник
```bash
rabbitmqctl delete_exchange <exchange_name>
```

## 4. Управление привязками

### Список всех привязок (связи очередь-обменник)
```bash
rabbitmqctl list_bindings
```

> **Примечание**: Привязка очереди к обменнику требует API или библиотеки клиента (например, `pika` в Python или HTTP API)

## 5. Управление пользователями и правами

### Список всех пользователей RabbitMQ
```bash
rabbitmqctl list_users
```

### Добавить нового пользователя RabbitMQ
```bash
rabbitmqctl add_user <username> <password>
```

### Удалить пользователя RabbitMQ
```bash
rabbitmqctl delete_user <username>
```

### Установить права для пользователя RabbitMQ
```bash
rabbitmqctl set_permissions -p <vhost> <username> ".*" ".*" ".*"
```

### Список прав для всех пользователей
```bash
rabbitmqctl list_permissions
```

### Изменить пароль пользователя
```bash
rabbitmqctl change_password <username> <new_password>
```

## 6. Виртуальные хосты

### Список всех виртуальных хостов
```bash
rabbitmqctl list_vhosts
```

### Добавить новый виртуальный хост
```bash
rabbitmqctl add_vhost <vhost_name>
```

### Удалить виртуальный хост
```bash
rabbitmqctl delete_vhost <vhost_name>
```

### Установить права пользователя на конкретном виртуальном хосте
```bash
rabbitmqctl set_permissions -p <vhost_name> <username> ".*" ".*" ".*"
```

## 7. Мониторинг и диагностика

### Показать переменные среды RabbitMQ
```bash
rabbitmqctl environment
```

### Показать работающие процессы
```bash
rabbitmqctl list_processes
```

### Просмотр подключений к RabbitMQ
```bash
rabbitmqctl list_connections
```

### Просмотр каналов для подключения
```bash
rabbitmqctl list_channels
```

### Закрыть конкретное подключение
```bash
rabbitmqctl close_connection <connection_name> "reason"
```

## 8. Управление кластером

### Добавить узел в кластер
```bash
rabbitmqctl join_cluster rabbit@<node_name>
```

### Добавить узел как RAM-узел
```bash
rabbitmqctl join_cluster --ram rabbit@<node_name>
```

### Удалить узел из кластера
```bash
rabbitmqctl forget_cluster_node rabbit@<node_name>
```

### Установить имя кластера
```bash
rabbitmqctl set_cluster_name <cluster_name>
```

## 9. Политики

### Список всех политик
```bash
rabbitmqctl list_policies
```

### Добавить или обновить политику
```bash
rabbitmqctl set_policy <policy_name> <pattern> '{"<key>": <value>}'
```

### Удалить конкретную политику
```bash
rabbitmqctl clear_policy <policy_name>
```

## 10. Управление Shovel и Federation

### Включить плагин Shovel (если еще не включен)
```bash
rabbitmq-plugins enable rabbitmq_shovel
```

### Список всех шовелов
```bash
rabbitmqctl list_parameters
```

### Добавить новую конфигурацию Shovel
```bash
rabbitmqctl set_parameter shovel <shovel_name> '{"src-uri": "amqp://", "dest-uri": "amqp://", "src-queue": "source_queue", "dest-queue": "destination_queue"}'
```

### Удалить конфигурацию Shovel
```bash
rabbitmqctl clear_parameter shovel <shovel_name>
```

## 11. Другие полезные команды

### Ротация log-файлов RabbitMQ
```bash
rabbitmqctl rotate_logs
```

### Показать справку по CLI RabbitMQ
```bash
rabbitmqctl help
```

### Показать справку для конкретной команды
```bash
rabbitmqctl help <command>
```

---

**Конец руководства по командам RabbitMQ**