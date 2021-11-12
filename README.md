# dockerzigbeemqttexporter

sudo apt install gnupg2 pass
docker image build -t dockerzigbeemqttexporter:latest  .
docker login -u revenberg
docker image push revenberg/dockerzigbeemqttexporter:latest

docker run revenberg/dockerzigbeemqttexporter

docker exec -it ??? /bin/sh

docker push revenberg/dockerzigbeemqttexporter:latest