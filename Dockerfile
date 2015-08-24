FROM python:2

MAINTAINER Philip Jay <phil@jay.id.au>

ENV TZ Australia/Melbourne

RUN pip install -U pip daemon

RUN mkdir /opt/aurora
ADD *.py /opt/aurora/

VOLUME /data

CMD [ "python", "/opt/aurora/aurora_reader.py", "-vvv", "-f", "/data/solar.sqlite" ]
