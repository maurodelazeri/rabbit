FROM node:15
LABEL maintainer="Mauro Delazeri <mauro@zinnion.com>"
WORKDIR /home/zinnion

COPY ./start.sh /usr/local/bin
COPY ./start_docs.sh /usr/local/bin
RUN chmod 775 /usr/local/bin/start.sh
RUN chmod 775 /usr/local/bin/start_docs.sh

RUN wget -O /usr/local/bin/mc https://dl.min.io/client/mc/release/linux-amd64/mc
RUN chmod +x /usr/local/bin/mc

EXPOSE 3000
