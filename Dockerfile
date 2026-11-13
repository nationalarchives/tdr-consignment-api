FROM alpine
#For alpine versions need to create a group before adding a user to the image


ARG SERVICE_NAME=api
ENV SERVICE_NAME=${SERVICE_NAME}

COPY --chmod=755 docker-entrypoint.sh /

WORKDIR ${SERVICE_NAME}

RUN addgroup --system ${SERVICE_NAME} && adduser --system ${SERVICE_NAME} -G ${SERVICE_NAME} && \
    apk update && \
    apk upgrade --no-cache p11-kit busybox libretls zlib libcrypto3 libssl3 openssl && \
    apk add --no-cache ca-certificates openssl && \
    mkdir -p /${SERVICE_NAME}/certs && \
    chown -R ${SERVICE_NAME} /${SERVICE_NAME} && \
    wget https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem && \
    apk add openjdk17 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/community

COPY target/scala-2.13/consignmentapi.jar /${SERVICE_NAME}/

USER ${SERVICE_NAME}

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD java -Dconfig.resource=application.$ENVIRONMENT.conf \
            -jar /api/consignmentapi.jar \
            -Dconsignmentapi.db.user=$DB_USER \
            -Dconsignmentapi.db.url=jdbc:mysql://$DB_URL:3306/consignmentapi \
            -Dauth.url=$AUTH_URL
