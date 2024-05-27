# syntax=docker/dockerfile:1

FROM python:3.12.3
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN make .
CMD [ "python" , "inicialize_db.py"]