# This Dockerfile is a stopgap solution until -Dpackaging=docker-native
# supports java 17 and Graal 21.3

FROM openjdk:17 AS build
WORKDIR /var/build
ADD src src
ADD .mvn .mvn
ADD pom.xml mvnw ./
RUN ./mvnw package

FROM openjdk:17
COPY --from=build /var/build/target/hdtss /usr/bin/
RUN mkdir /data
VOLUME /data
WORKDIR /data
ENTRYPOINT ["/usr/bin/hdtss"]

