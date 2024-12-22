import json
import time
from datetime import datetime

import numpy as np
import pika
from sklearn.datasets import load_diabetes

# Создаём бесконечный цикл для отправки сообщений в очередь
while True:
    # Загружаем датасет о диабете
    X, y = load_diabetes(return_X_y=True)
    # Формируем случайный индекс строки
    random_row = np.random.randint(0, X.shape[0]-1)

    # Создаём подключение по адресу rabbitmq:
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    message_id = datetime.timestamp(datetime.now())
    message_y_true = {
        'id': message_id,
        'body': y[random_row]
    }
    message_features = {
        'id': message_id,
        'body': list(X[random_row])
    }

    # Создаём очередь y_true
    channel.queue_declare(queue='y_true')
    # Создаём очередь features
    channel.queue_declare(queue='features')

    # Публикуем сообщение в очередь y_true
    channel.basic_publish(exchange='',
                        routing_key='y_true',
                        body=json.dumps(message_y_true))
    print('Сообщение с правильным ответом отправлено в очередь')

    # Публикуем сообщение в очередь features
    channel.basic_publish(exchange='',
                        routing_key='features',
                        body=json.dumps(message_features))
    print('Сообщение с вектором признаков отправлено в очередь')

    # Закрываем подключение
    connection.close()

    time.sleep(10)