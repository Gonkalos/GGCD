# Move to project directory
cd Parser/

# Build Maven project
#mvn package

# Dump output to terminal
FILE=sorted/part-r-00000

if test -f "$FILE"; 
then
    cat sorted/part-r-00000
    rm -rf output
    rm -rf sorted
else
    cat output/part-r-00000
    rm -rf output
fi