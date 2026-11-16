FROM alpine
#For alpine versions need to create a group before adding a user to the image
WORKDIR /api
RUN addgroup --system apigroup && adduser --system apiuser -G apigroup && \
    apk update && \
    apk upgrade --no-cache p11-kit busybox libretls zlib libcrypto3 libssl3 openssl && \
    apk add ca-certificates && \
    chown -R apiuser /api && \
    wget https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem && \
    apk add openjdk17 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community
COPY target/scala-2.13/consignmentapi.jar /api

USER apiuser
# The JVM defaults to a maximum heap of 25% of the container memory limit, which leaves most of the
# task's memory unused. The three -D arguments that used to follow -jar were passed to the application
# rather than the JVM, and ApiServer ignores its arguments, so they had no effect and have been removed.
CMD java -Dconfig.resource=application.$ENVIRONMENT.conf \
            -XX:MaxRAMPercentage=75.0 \
            -XX:+ExitOnOutOfMemoryError \
            -jar /api/consignmentapi.jar
