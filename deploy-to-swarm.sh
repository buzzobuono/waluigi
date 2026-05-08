#!/bin/bash

docker stack deploy -c docker-compose.yml waluigi

docker ps -a