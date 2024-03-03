#!/bin/bash

docker stop $(docker ps -aq)
docker container rm $(docker ps -aq)
docker network prune -f
docker volume ls -q | xargs docker volume rm