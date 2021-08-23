FROM osgeo/gdal:latest

RUN apt-get update

RUN apt-get install python python3-pip \
    libpq-dev postgresql postgresql-contrib \
    unixodbc-dev python3-tk -y

WORKDIR /usr/src

COPY requirements.txt .

ENV FLASK_APP=/usr/src/index
ENV FLASK_ENV=development

RUN pip3 install --no-cache-dir -r requirements.txt

COPY / .

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]