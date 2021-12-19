FROM python:3.8-slim-buster
WORKDIR usr/src/mlb-api
COPY requirements.txt .
RUN pip install -r requirements.txt
EXPOSE 5001
COPY . .
CMD ["gunicorn", "--timeout", "60", "--worker-class", "gevent", "--workers", "3", "--bind", "0.0.0.0:5001", "wsgi:app", "--log-level=debug", "--log-file=-"]