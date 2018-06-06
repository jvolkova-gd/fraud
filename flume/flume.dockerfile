FROM probablyfine/flume
MAINTAINER jvolkova@griddynamics.com
ENV FLUME_AGENT_NAME a1
ADD ./flume.conf /opt/flume-config/flume.conf
ADD ./target/jsoninterceptor.jar /opt/flume/lib/jsoninterceptor.jar
EXPOSE 4444

ENTRYPOINT [ "flume-ng", "agent", "-c", "/opt/flume/conf", "-f", "/opt/flume-config/flume.conf", "-n", "a1", "-Dflume.root.logger=DEBUG,console" ]
