FROM python:3.8
USER 0
WORKDIR /usr/src/mlb-api
COPY requirements.txt .
RUN chown -R 1001:0 /usr/src/mlb-api
RUN chgrp -R 0 /usr/src/mlb-api && chmod -R g=u /usr/src/mlb-api
USER 1001
RUN pip install --upgrade pip setuptools wheel
RUN pip install -r requirements.txt
EXPOSE 5001
COPY . .
CMD ["gunicorn", "--timeout", "60", "--worker-class", "gevent", "--workers", "3", "--bind", "0.0.0.0:5001", "wsgi:app", "--log-level=debug", "--log-file=-"]