# Clone repository
git clone https://github.com/big-data-europe/docker-spark.git

# Add network configuration to docker-compose.yml
# networks:
#   default:
#     external:
#       name: docker-hadoop_default

# 
docker-compose up


docker rm spark-master spark-worker-1