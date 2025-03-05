from airflow.providers.telegram.hooks.telegram import TelegramHook

def send_telegram_failure_message(context):

        hook = TelegramHook(token='7245437813:AAG7Z74tzM6_l40eJEeyAcmxmAxfxLMbj2E', 
                        chat_id='-4721938541')

        run_id = context['run_id']
        dag_id = context['task_instance_key_str']
    
        message = f'Исполнение DAG {dag_id} с id={run_id} прошло с ошибкой!'
        hook.send_message({
            'chat_id': '-4721938541',
            'text': message
        })


def send_telegram_success_message(context):

        hook = TelegramHook(token='7245437813:AAG7Z74tzM6_l40eJEeyAcmxmAxfxLMbj2E', 
                        chat_id='-4721938541')

        run_id = context['run_id']
        dag_id = context['task_instance_key_str']
    
        message = f'Исполнение DAG {dag_id} с id={run_id} прошло успешно!'
        hook.send_message({
            'chat_id': '-4721938541',
            'text': message
        }) 