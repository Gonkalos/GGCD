# Deploy Hadoop cluster

git clone https://github.com/big-data-europe/docker-hadoop.git
cd docker-hadoop
sudo docker-compose pull
sudo docker-compose up

# Load files into HDFS

# Load data files
docker run --env-file hadoop.env --network docker-hadoop_default -v /Users/goncalo/Documents/University/GGCD/Hadoop/Data:/data -it bde2020/hadoop-base hdfs dfs -put /data/title.basics.tsv.gz /
docker run --env-file hadoop.env --network docker-hadoop_default -v /Users/goncalo/Documents/University/GGCD/Hadoop/Data:/data -it bde2020/hadoop-base hdfs dfs -put /data/title.ratings.tsv.gz /

# Load schemas
docker run --env-file hadoop.env --network docker-hadoop_default -v /Users/goncalo/Documents/University/GGCD/Hadoop/Docker/App:/schemas -it bde2020/hadoop-base hdfs dfs -put /schemas/movie_schema.parquet /
docker run --env-file hadoop.env --network docker-hadoop_default -v /Users/goncalo/Documents/University/GGCD/Hadoop/Docker/App:/schemas -it bde2020/hadoop-base hdfs dfs -put /schemas/projection_schema.parquet /
docker run --env-file hadoop.env --network docker-hadoop_default -v /Users/goncalo/Documents/University/GGCD/Hadoop/Docker/App:/schemas -it bde2020/hadoop-base hdfs dfs -put /schemas/year_schema.parquet /

# Launch a Bash terminal within a container
docker exec -it namenode bash

# Load files from the Web
# curl --output - https://datasets.imdbws.com/title.basics.tsv.gz | hdfs dfs -put - /title.basics.tsv.gz
# curl --output - https://datasets.imdbws.com/title.ratings.tsv.gz | hdfs dfs -put - /title.ratings.tsv.gz

# hdfs dfs -ls /

# Build and run

# Move to project directory
cd ../App/
# Build Maven project
mvn package
# Build Docker image from Dockerfile
docker build -t test .
# Run Docker image
docker run -it --env-file /Users/goncalo/Documents/University/GGCD/Hadoop/Docker/docker-hadoop/hadoop.env --network docker-hadoop_default test
# Launch a Bash terminal within a container
docker exec -it namenode bash
# See output
# hdfs dfs -cat /from_parquet_output/part-r-00000
# hdfs dfs -cat /validate_years_output/part-r-00000

# Clean up

docker-compose down --volumes