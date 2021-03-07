# Load title.basics.tsv into HDFS

# Move to project directory
cd docker-hadoop

# Uploading local file
docker run --env-file hadoop.env \
--network docker-hadoop_default \
-v /Users/goncalo/Documents/University/Year\ 4/CD/GGCD/Classes/IMDb\ Datasets/Mini:/data \
-it bde2020/hadoop-base \
hdfs dfs -put /data/title.basics.tsv.bz2 /

# Launch a Bash terminal within a container
docker exec -it namenode bash

# Loading file from the Web
# curl --output - https://datasets.imdbws.com/title.basics.tsv.gz | hdfs dfs -put - /title.basics.tsv.gz

# hdfs dfs -ls /