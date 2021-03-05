# Move to project directory
cd Parser/

# Build Maven project
mvn package

# Build Docker image from Dockerfile
docker build -t test .

# Run process binding folder in host to folder in container
docker run -it \
-v /Users/goncalo/Documents/University/Year\ 4/CD/GGCD/Classes/IMDb\ Datasets:/data \
test /data/Mini/title.basics.tsv.bz2 /data/Mini/title.principals.tsv.bz2