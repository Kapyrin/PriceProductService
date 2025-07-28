FROM openjdk:21
LABEL authors="vladimirkapyrin"
WORKDIR /app
COPY build/libs/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]