FROM maven:3.9.9-eclipse-temurin-21 AS builder

WORKDIR /app
COPY pom.xml ./
# Pre-fetch dependencies (layer cache)
RUN mvn -q dependency:go-offline -Dmaven.repo.local=/root/.m2
COPY src ./src
RUN mvn -q -DskipTests package -Dmaven.repo.local=/root/.m2

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/battery-producer-0.0.1-SNAPSHOT.jar app.jar

ENV SERVER_PORT=8086
EXPOSE 8086

ENTRYPOINT ["java", "-jar", "/app/app.jar"]

