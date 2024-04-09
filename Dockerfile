FROM python:3.11

WORKDIR /app

RUN sudo apt-get update

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .