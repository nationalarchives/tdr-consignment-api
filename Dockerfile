FROM openjdk:15-jdk-alpine
#For alpine versions need to create a group before adding a user to the image
WORKDIR /api
RUN addgroup --system apigroup && adduser --system apiuser -G apigroup && \
    apk update && \
    apk upgrade p11-kit && \
    apk add ca-certificates && \
    chown -R apiuser /api && \
    wget https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem
COPY target/scala-2.13/consignmentapi.jar /api

USER apiuser
CMD java -Dconfig.resource=application.$ENVIRONMENT.conf \
            -jar /api/consignmentapi.jar \
            -Dconsignmentapi.db.user=$DB_USER \
            -Dconsignmentapi.db.url=jdbc:mysql://$DB_URL:3306/consignmentapi \
            -Dauth.url=$AUTH_URL
