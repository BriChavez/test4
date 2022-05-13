#!/bin/bash
# start mariadb docker container with empty database named spotify
docker run --name mariadb \
    -p 127.0.0.1:3306:3306 \
    -e MYSQL_ROOT_PASSWORD=mysql \
    -e MYSQL_DATABASE=spotify \
    --rm -d \
    mariadb:latest

# docker exec -it mariadb mysql -D spotify -u root -pmysql
# Install Requirements
# Create a Python virtual environment
# Activate it
# pip install the following packages: bash numpy==1.20.2 pandas==1.2.4 SQLAlchemy==1.4.13 PyMySQL==1.0.2 jupyterlab==3.0.14 notebook==6.3.0
# ...or copy the requirements.txt file from ch3/ep3 and install its contents
