import json

import pandas as pd
import pika

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
   
    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')
 
    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        message = json.loads(body)
        value = message['body']
        
        answer_string = f'Из очереди {method.routing_key} получено значение {value}'
        with open('./logs/labels_log.txt', 'a') as log:
            log.write(answer_string +'\n')

        data = pd.read_csv('./logs/metric_log.csv', index_col='id')
        if message['id'] not in data.index:
            new_entry = pd.DataFrame(
                data={
                    'y_true': value if method.routing_key == 'y_true' else pd.NA,
                    'y_pred': value if method.routing_key == 'y_pred' else pd.NA,
                    'absolute_error': pd.NA
                },
                index=message['id']
            )
            data = pd.concat(data, new_entry)
        else:
            data.loc[message['id']][method.routing_key] = value
            ae = abs(data.loc[message['id']]['y_true'] - data.loc[message['id']]['y_pred'])
            data.loc[message['id']]['absolute_error'] = ae
        data.to_csv('./logs/metrics_log.csv')

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )
 
    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except:
    print('Не удалось подключиться к очереди')
