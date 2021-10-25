# This Dockerfile is a stopgap solution until -Dpackaging=docker-native
# supports java 17 and Graal 21.3

FROM findepi/graalvm:java17-native AS build
WORKDIR /var/build
ADD src src
ADD .mvn .mvn
ADD pom.xml mvnw ./
RUN ./mvnw verify && \
    ./mvnw package -Dpackaging=native-image

FROM debian:stable-slim
COPY --from=build /var/build/target/hdtss /usr/bin/
RUN mkdir /data
VOLUME /data
WORKDIR /data
ENTRYPOINT ["/usr/bin/hdtss"]

