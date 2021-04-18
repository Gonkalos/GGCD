FROM bde2020/hadoop-base
COPY target/app-1.0-SNAPSHOT.jar /
CMD ["/spark/bin/spark-submit", "--class", "ActorMovieCount", "--master", "spark://spark-master:7077", "/app-1.0-SNAPSHOT.jar"]