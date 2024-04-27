FROM python:3.12-alpine
WORKDIR /app
COPY requirements.txt /app
COPY app.py /app
RUN apk add py3-pip
RUN apk add --update py3-pip
RUN apk add build-base
RUN apk add postgresql-dev gcc python3-dev musl-dev && pip3 install psycopg2
RUN pip install -r requirements.txt
CMD ["python3", "app.py"]
