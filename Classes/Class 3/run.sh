# Move to project directory
cd App/

# Build Maven project
mvn package

# Build Docker image from Dockerfile
docker build -t test .

# Run Docker image
docker run -it \
--env-file /Users/goncalo/Documents/University/GGCD/Classes/Class\ 3/docker-hadoop/hadoop.env \
--network docker-hadoop_default test

# Launch a Bash terminal within a container
docker exec -it namenode bash

# See output
# hdfs dfs -cat /output/part-r-00000