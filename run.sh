#!/bin/bash

source my_fin/bin/activate

echo "Start docker DB"
docker start myfin

echo "Running crawler banks"
scrapy crawl banks

echo "Running crawler nbrb"
scrapy crawl nbrb

cd analytics
files=$(ls *.py)
for file in $files
do
    echo "Running $file"
    python3 $file
done

echo "Stop docker DB"
docker stop myfin
