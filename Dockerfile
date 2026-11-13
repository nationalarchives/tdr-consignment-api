# Corretto 21.0.13.1.2; pin the multi-architecture image containing the security fix.
FROM amazoncorretto:21.0.13-al2023@sha256:3284a05cb82e9bceb4857f9d70788a6f72681d9070b550ae5f477b5e3dc81a5c
ARG SERVICE_NAME=api
ENV SERVICE_NAME=${SERVICE_NAME}
WORKDIR /api
COPY --chmod=755 docker-entrypoint.sh /
RUN dnf install -y ca-certificates shadow-utils openssl && \
    dnf clean all && \
    groupadd --system apigroup && \
    useradd --system --gid apigroup --no-create-home apiuser && \
    mkdir -p /api/certs && \
    curl --fail --silent --show-error --location \
        --output /api/eu-west-2-bundle.pem \
        https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem && \
    chown -R apiuser:apigroup /api
COPY target/scala-2.13/consignmentapi.jar /api

USER apiuser
ENTRYPOINT ["/docker-entrypoint.sh"]
# Override the JVM's default maximum heap of 25% of container memory.
# Retain the CVE-2026-114938 mitigation as defense in depth.
CMD ["sh", "-c", "exec java -XX:-UseBASE64Intrinsics \
            \"-Dconfig.resource=application.$ENVIRONMENT.conf\" \
            -XX:MaxRAMPercentage=75.0 \
            -XX:+ExitOnOutOfMemoryError \
            -jar /api/consignmentapi.jar"]
