#!/bin/bash

source VenvZion17/bin/activate

echo "Start docker DB"
docker-compose up -d

# echo "Running crawler banks"
# scrapy crawl banks

echo "Running crawler nbrb"
scrapy crawl nbrb

cd analytics

echo "Running main.py"
python3 main.py

echo "Stop docker DB"
docker-compose down
