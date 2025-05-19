import logging
import os
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from pytz import utc

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from database.database import connection_string
from helpers.email_utils import send_log_email

jobstores = {
    'default': SQLAlchemyJobStore(url=connection_string)
}

executors = {
    'default': ThreadPoolExecutor(2),
    'processpool': ProcessPoolExecutor(2)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}


def setup_logging():
    todays_date = datetime.now().strftime("%m-%d-%Y")
    log_dir = os.path.join("./logs", todays_date)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "execution.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )
    return log_file


def execute_with_logging(func, *args, **kwargs):
    log_file = setup_logging()
    recipient_email = os.getenv("LOG_RECIPIENT_EMAIL", "elijahanderson96@gmail.com")
    try:
        func(*args, **kwargs)
        logging.info("Script execution started.")
        logging.info("Script executed successfully.")
        send_log_email(log_file, recipient_email)
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        send_log_email(log_file, recipient_email)  # Still send logs in case of failure
        raise


def mysillyfunc(*args, **kwargs):
    print('hello word')
    print(args)
    print(kwargs)


scheduler = BlockingScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=utc)
scheduler.add_job(mysillyfunc, args=['asdfas', 'asdfasdf'], kwargs={'hello': 'stranger', 'whats': 'good'},
                  trigger='interval', minutes=1, max_instances=1, id='mysillyfunc')
scheduler.start()
scheduler.print_jobs()


