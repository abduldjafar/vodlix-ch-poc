docker compose down
docker rm $(docker ps -a | grep vodlix | grep Exited | awk '{ print $1 }')
docker rmi $(docker images | grep vodlix | awk '{ print $3 }')
docker compose up