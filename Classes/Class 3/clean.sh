# Kill Docker containers
docker kill historyserver namenode resourcemanager nodemanager datanode

# Remove Docker containers
docker rm historyserver namenode resourcemanager nodemanager datanode

# Remove Docker volumes
docker volume rm docker-hadoop_hadoop_datanode docker-hadoop_hadoop_historyserver docker-hadoop_hadoop_namenode