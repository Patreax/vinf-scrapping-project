#!/bin/bash

docker-compose down
docker-compose build lucene-indexer
docker-compose run --rm lucene-indexer
docker-compose build lucene-searcher
docker-compose run --rm lucene-searcher