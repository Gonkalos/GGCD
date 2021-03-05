# Load title.basics.tsv.gz into HDFS

cd docker-hadoop

# Loading files from the Web
#docker exec -it namenode bash
#curl --output - https://storage.googleapis.com/ggcdimdb/mini/title.basics.tsv.bz2 | hdfs dfs -put - /title.basics.tsv.bz2

# Loading files from the Web and decompressing GZip on-the-fly
#docker exec -it namenode bash
#curl --output - https://datasets.imdbws.com/name.basics.tsv.gz | gzip -d | hdfs dfs -put - /name.basics.tsv.gz

# Uploading local file
docker run --env-file hadoop.env \
--network docker-hadoop_default \
-v /Users/goncalo/Documents/University/Year\ 4/CD/GGCD/Classes/IMDb\ Datasets/Mini:/data \
-it bde2020/hadoop-base \
hdfs dfs -put /data/title.basics.tsv.bz2 /title.basics.tsv.bz2

docker exec -it namenode bash

#hdfs dfs -ls /