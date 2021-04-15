FROM bde2020/hadoop-base
COPY target/app-1.0-SNAPSHOT.jar /
CMD ["hadoop", "jar", "/app-1.0-SNAPSHOT.jar", "ToParquet"]
#CMD ["hadoop", "jar", "/app-1.0-SNAPSHOT.jar", "FromParquet"]
#CMD ["hadoop", "jar", "/app-1.0-SNAPSHOT.jar", "ComputeYears"]
#CMD ["hadoop", "jar", "/app-1.0-SNAPSHOT.jar", "ValidateYears"]