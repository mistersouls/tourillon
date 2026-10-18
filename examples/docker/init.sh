#!/bin/sh
set -eu

umask 077
cd /example

if [ ! -f ./certs/ca-cert.pem ] && [ ! -f ./certs/ca-key.pem ]; then
    tourillon config ca ./certs
fi

if [ ! -f ./node.toml ]; then
    tourillon config node node-1 ./data 8Mi \
        ./certs/ca-cert.pem ./certs/ca-key.pem \
        --output-file ./node.toml \
        --client-bind 0.0.0.0:7000 \
        --peer-bind 0.0.0.0:7001 \
        --peer-advertise tourillon:7001
fi

if [ ! -f ./tourconf.toml ]; then
    tourillon config client local-user \
        --ca-cert ./certs/ca-cert.pem \
        --ca-key ./certs/ca-key.pem \
        --output-file ./tourconf.toml \
        --client-address tourillon:7000 \
        --peer-address tourillon:7001
fi
