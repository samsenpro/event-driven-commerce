# syntax=docker/dockerfile:1
#
# Imagen común para todos los servicios. Uso:
#   docker build --build-arg SERVICE=order-service -t order-service .

# ---- Build ----
FROM maven:3.9-eclipse-temurin-21 AS build
ARG SERVICE
WORKDIR /workspace

COPY . .
# Compila solo el servicio y los módulos de los que depende (-am). La caché de ~/.m2 se comparte
# entre las construcciones de todos los servicios.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -q -DskipTests -pl services/${SERVICE} -am package \
    && cp services/${SERVICE}/target/${SERVICE}-*.jar /workspace/app.jar

# ---- Runtime ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=build --chown=app:app /workspace/app.jar app.jar

ENV SPRING_PROFILES_ACTIVE=docker \
    SERVER_PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=40s --retries=6 \
    CMD wget -qO- http://localhost:8080/actuator/health/readiness || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
