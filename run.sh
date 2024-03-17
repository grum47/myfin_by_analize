#!/bin/bash

source /home/tests/tests/vscode/Zion17/my_fin/bin/activate

echo "Start docker DB"
docker start myfin

# echo "Running crawler banks"
# scrapy crawl banks

echo "Running crawler nbrb"
scrapy crawl nbrb

cd /home/tests/tests/vscode/Zion17/analytics

echo "Running main.py"
python3 main.py

echo "Stop docker DB"
docker stop myfin
