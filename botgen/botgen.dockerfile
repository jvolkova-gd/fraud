FROM python:3.6-stretch
MAINTAINER jvolkova@griddynamics.com
COPY ./botgen.py ./botgen.py
ENTRYPOINT ["python", "botgen.py", "--bots=99", "--users=1200", "--cats=15", "--duration=5", "--file=/data/source.json"]