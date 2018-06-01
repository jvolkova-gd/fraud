FROM python:3.6-stretch
MAINTAINER jvolkova@griddynamics.com
COPY ./botgen.py
ADD
RUN mkdir ./source_data
RUN touch ./source_data/data.json
RUN python botgen.py --bots=99  --users=1200 --cats=15 --duration=5 >> ./source_data/data.json

ENTRYPOINT ["botgen"]
WORKDIR ./
EXPOSE 9996
ENV
VOLUME