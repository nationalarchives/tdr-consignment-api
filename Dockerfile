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
CMD [
      "java",
      "-Dconfig.resource=application.$ENVIRONMENT.conf",
      "-jar",
      "/api/consignmentapi.jar",
      "-Dconsignmentapi.db.user=$DB_USER",
      "-Dconsignmentapi.db.url=jdbc:mysql://$DB_URL:3306/consignmentapi",
      "-Dauth.url=$AUTH_URL"
    ]

