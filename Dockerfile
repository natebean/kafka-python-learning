FROM python:3.7
RUN apt-get update && apt-get install -y --no-install-recommends python-virtualenv python-dev librocksdb-dev
COPY requirements.txt /
RUN pip install -r /requirements.txt
# COPY src/ /app
# WORKDIR /app