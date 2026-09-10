# alert-service: consumes the alerts topic, persists idempotently, serves the API.
#
#   docker build -f alert-service/Dockerfile -t alert-service alert-service
#
# Java 17 for both stages. The workstation this was developed on runs JDK 21, so
# building in the container is what guarantees the artefact really targets 17
# rather than depending on whoever happens to run the build.

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build

# Dependencies first, as their own layer: the pom changes far less often than
# the sources, so a code edit does not re-download the world.
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src src
# Tests are not run here. They need a Docker daemon for Testcontainers, which is
# not available inside a build container, and running them in CI where a daemon
# does exist is the honest place for them.
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Unprivileged: the service opens a port and talks to a database, and needs
# nothing that root provides.
RUN groupadd --system alert && useradd --system --gid alert --home /app alert

COPY --from=build /build/target/alert-service-*.jar /app/alert-service.jar
RUN chown -R alert:alert /app
USER alert

EXPOSE 8080

# The readiness probe, not liveness. It reports DOWN while Flyway is still
# migrating, so an orchestrator that waits on this container waits for a schema
# that is actually there -- "started" is not "ready".
#
# Declared in the image and not only in compose: an image that cannot say
# whether it is working is incomplete, and compose is not the only thing that
# will ever run it.
HEALTHCHECK --interval=10s --timeout=5s --start-period=40s --retries=12 \
  CMD wget -q -O - http://127.0.0.1:8080/actuator/health/readiness | grep -q UP || exit 1

# Container-aware heap sizing rather than a fixed -Xmx: the JVM reads the cgroup
# limit, so the same image behaves correctly whatever memory compose grants it.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/alert-service.jar"]
