FROM python:3.9
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /usr/src/app
COPY ./src/ ./
CMD [ "python", "./metric.py" ]