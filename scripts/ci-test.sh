#!/bin/bash
set -e

echo "aster start tests......"

# cargo test

docker run -e "IP=0.0.0.0" -d -p 7000-7007:7000-7007 grokzen/redis-cluster:5.0.7 && cargo test --verbose --all

sudo apt install python3 -y
sudo pip install pytest mock python-toml

git clone --depth=1 https://github.com/wayslog/redis-py.git
cp default.toml redis-py

make debug &
sleep 5

cd redis-py

pytest --redis-url="redis://127.0.0.1:7787"
pytest --redis-url="redis://127.0.0.1:7788"