FROM python:3.11-slim

WORKDIR /app

COPY config /app/config
COPY edgar /app/edgar
COPY database /app/database
COPY helpers /app/helpers
COPY ui_api /app/ui_api
COPY scripts /app/scripts
COPY utils /app/utils
COPY *.py /app/
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r /app/requirements.txt

# 2n + 1 for cpu count
CMD ["uvicorn", "ui_api.main:app", "--host", "0.0.0.0", "--port", "80", "--workers", "1"]


