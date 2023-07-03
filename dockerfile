FROM python:3.9

RUN apt update

RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash -
RUN apt-get install -y nodejs postgresql-client netcat-traditional

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]