# Corretto 21.0.13.1.2; pin the multi-architecture image containing the security fix.
FROM amazoncorretto:21.0.13-al2023@sha256:3284a05cb82e9bceb4857f9d70788a6f72681d9070b550ae5f477b5e3dc81a5c
WORKDIR /api
RUN dnf install -y ca-certificates shadow-utils && \
    dnf clean all && \
    groupadd --system apigroup && \
    useradd --system --gid apigroup --no-create-home apiuser && \
    curl --fail --silent --show-error --location \
        --output /api/eu-west-2-bundle.pem \
        https://truststore.pki.rds.amazonaws.com/eu-west-2/eu-west-2-bundle.pem && \
    chown -R apiuser:apigroup /api
COPY target/scala-2.13/consignmentapi.jar /api

USER apiuser
# Retain the CVE-2026-114938 mitigation as defense in depth.
CMD ["sh", "-c", "exec java -XX:-UseBASE64Intrinsics \
            \"-Dconfig.resource=application.$ENVIRONMENT.conf\" \
            -jar /api/consignmentapi.jar \
            \"-Dconsignmentapi.db.user=$DB_USER\" \
            \"-Dconsignmentapi.db.url=jdbc:mysql://$DB_URL:3306/consignmentapi\" \
            \"-Dauth.url=$AUTH_URL\""]
