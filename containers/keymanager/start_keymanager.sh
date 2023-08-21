#!/bin/bash

if [[ $# -lt 2 ]] ; then
    echo "[ERROR]: Not enough arguments supplied"
    echo "Usage:"
    echo "./start_metget_keymanager.sh [namespace] [prefix]"
    exit 1
fi

namespace=$1
prefix=$2

echo "[INFO]: Starting key manager in namespace: $1 with prefix: $2"
kubectl -n $1 exec -it $2-keymanager -- /bin/bash
