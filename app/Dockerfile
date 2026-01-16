FROM europe-north1-docker.pkg.dev/cgr-nav/pull-through/nav.no/jre:openjdk-25
LABEL maintainer="team-researchops"
COPY build/libs/app.jar app.jar
EXPOSE 8080
CMD ["-jar", "app.jar"]