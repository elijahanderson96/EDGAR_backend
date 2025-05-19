import asyncio
import io
import logging
import os
import time
from datetime import datetime

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.schedulers.blocking import BlockingScheduler
from pytz import utc

from helpers.email_utils import send_log_email  # Assuming this exists and works

from scripts.refresh import main as refresh_main  # Assuming this takes (logger, *args)
from scripts.historical_prices import main as historical_prices_main  # Assuming this is async and takes (logger, *args)

executors = {
    'default': ThreadPoolExecutor(4),
    'processpool': ProcessPoolExecutor(2)
}

job_defaults = {
    'coalesce': False,
    'max_instances': 3
}


# --- Main Application Logging Setup ---
def setup_main_logging():
    todays_date = datetime.now().strftime("%Y-%m-%d")
    log_dir = os.path.join("./logs", "scheduler_main", todays_date)
    os.makedirs(log_dir, exist_ok=True)
    main_log_file = os.path.join(log_dir, f"scheduler_main_process_{os.getpid()}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    if not any(
            isinstance(h, logging.StreamHandler) and h.stream == console_handler.stream for h in root_logger.handlers):
        root_logger.addHandler(console_handler)

    file_handler = logging.FileHandler(main_log_file)
    file_handler.setFormatter(formatter)
    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == file_handler.baseFilename for h in
               root_logger.handlers):
        root_logger.addHandler(file_handler)

    logging.info(f"Main application logging configured. Main log file: {main_log_file}")
    return main_log_file


# --- Job Execution and Logging Wrapper ---
def execute_job_and_send_log(job_func, job_id, *args, **kwargs):
    recipient_email = os.getenv("LOG_RECIPIENT_EMAIL", "your_email@example.com")  # Replace
    log_buffer = io.StringIO()

    # Timestamp for this specific job run
    run_datetime = datetime.now()
    run_timestamp_str = run_datetime.strftime('%Y%m%d_%H%M%S_%f')
    run_date_str = run_datetime.strftime('%Y-%m-%d')

    # Create a unique logger name for this specific job execution
    job_logger_name = f"job.{job_id}.{run_timestamp_str}"
    job_logger = logging.getLogger(job_logger_name)
    job_logger.setLevel(logging.INFO)

    # Prevent job logs from propagating to the root logger (console, main scheduler log)
    # Set to True if you want job-specific logs ALSO in the main console/scheduler log
    job_logger.propagate = False

    # --- Setup Handler 1: StringIO buffer (for email) ---
    buffer_handler = logging.StreamHandler(log_buffer)
    email_formatter = logging.Formatter("%(asctime)s - %(levelname)s (%(name)s) - %(message)s")
    buffer_handler.setFormatter(email_formatter)
    job_logger.addHandler(buffer_handler)

    # --- Setup Handler 2: FileHandler (for job-specific file) ---
    job_log_dir = os.path.join("./logs", "job_logs", job_id, run_date_str)
    os.makedirs(job_log_dir, exist_ok=True)
    job_log_file_path = os.path.join(job_log_dir, f"{job_id}_run_{run_timestamp_str}.log")

    job_file_handler = logging.FileHandler(job_log_file_path)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s (%(name)s) - %(message)s")
    job_file_handler.setFormatter(file_formatter)
    job_logger.addHandler(job_file_handler)

    # Log to root logger (goes to console and main scheduler log)
    logging.info(f"Starting execution of job ID: {job_id}. Log file: {job_log_file_path}")
    start_time = time.time()

    subject = ""  # Initialize subject
    try:
        # Pass the job-specific logger to the actual job function
        job_func(job_logger, *args, **kwargs)  # job_logger is passed here
        end_time = time.time()
        # Log to root logger
        logging.info(f"Job ID: {job_id} executed successfully in {end_time - start_time:.2f} seconds.")
        # Log to job-specific logger (goes to buffer and job file)
        job_logger.info("Job execution completed successfully.")
        subject = f"SUCCESS: Job '{job_id}' Completed ({run_datetime.strftime('%Y-%m-%d %H:%M:%S')})"
    except Exception as e:
        end_time = time.time()
        # Log to root logger
        logging.error(f"Job ID: {job_id} failed after {end_time - start_time:.2f} seconds: {e}",
                      exc_info=False)  # exc_info=True for full stack in main log
        # Log to job-specific logger (goes to buffer and job file), including full traceback
        job_logger.error(f"Job execution failed: {e}", exc_info=True)
        subject = f"FAILURE: Job '{job_id}' Failed ({run_datetime.strftime('%Y-%m-%d %H:%M:%S')})"
    finally:
        # Close and remove handlers from job_logger
        job_logger.removeHandler(buffer_handler)
        buffer_handler.close()

        job_logger.removeHandler(job_file_handler)
        job_file_handler.close()

        # Get the log content from the buffer for email
        log_contents = log_buffer.getvalue()
        log_buffer.close()

        # Send the email with the captured logs
        if recipient_email and subject:  # Only send if email is configured and subject is set
            try:
                logging.info(f"Attempting to send log email for job ID: {job_id} to {recipient_email}")
                # Use the job-specific log file name for the attachment name for consistency
                attachment_filename = os.path.basename(job_log_file_path)
                send_log_email(subject=subject,
                               body_text=f"Job '{job_id}' execution details attached.\nLog file path on server: {job_log_file_path}",
                               attachment_content=log_contents,
                               attachment_filename=attachment_filename,
                               recipient_email=recipient_email)
                logging.info(f"Log email sent for job ID: {job_id}")
            except Exception as email_exc:
                logging.error(f"Failed to send log email for job ID: {job_id}: {email_exc}", exc_info=True)
        elif not subject:
            logging.warning(f"No subject set for job ID: {job_id}, email not sent.")


# --- Job Functions (must accept logger as first argument) ---
def refresh_func(logger):  # logger is the job_logger from the wrapper
    logger.info(f"Data refresh process starting inside refresh_func.")
    # Example: Call your actual main refresh logic, passing the logger
    refresh_main(logger)  # Your actual refresh_main must accept logger
    logger.info(f"Data refresh process completed inside refresh_func.")


def refresh_historical_prices_fun(logger):  # logger is the job_logger
    logger.info(f"Historical prices refresh process starting inside refresh_historical_prices_fun.")
    # IMPORTANT: historical_prices_main MUST accept logger as its first argument
    # and be an async function if you use asyncio.run.
    asyncio.run(historical_prices_main(logger, start_date='2000-01-01', end_date=None))
    logger.info(f"Historical prices refresh process completed inside refresh_historical_prices_fun.")


# --- Main Execution ---
if __name__ == "__main__":
    setup_main_logging()

    logging.info("Initializing APScheduler...")
    scheduler = BlockingScheduler(
        executors=executors,
        job_defaults=job_defaults,
        timezone=utc
    )

    job_id_1 = 'refresh_company_facts_job'
    scheduler.add_job(
        execute_job_and_send_log,
        trigger='interval',
        minutes=1,  # For testing, set to a short interval
        id=job_id_1,
        max_instances=1,
        args=[refresh_func, job_id_1],  # 1st arg to execute_job_and_send_log is job_func, 2nd is job_id
        # Additional args for refresh_func itself would go after job_id_1
    )

    job_id_2 = 'refresh_historical_data_job'
    scheduler.add_job(
        execute_job_and_send_log,
        trigger='interval',
        minutes=1,  # For testing
        id=job_id_2,
        max_instances=1,
        args=[refresh_historical_prices_fun, job_id_2],
    )

    logging.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logging.info("Scheduler shutting down...")
    finally:
        if scheduler.running:
            scheduler.shutdown()
        logging.info("Scheduler shutdown complete.")
