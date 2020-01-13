#!/bin/sh

docker run --rm=true -it -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres postgres
