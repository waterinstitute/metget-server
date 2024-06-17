#!/bin/bash

if [[ $# -ne 1 ]] ; then
    echo "[ERROR]: Not enough arguments supplied"
    echo "Usage:"
    echo "./start_metget_keymanager.sh [namespace]"
    exit 1
fi

namespace=$1
prefix=$2
pod_name=$(kubectl get pods -l=app=keymanager -n $1 --no-headers | cut -d" " -f1)

echo "[INFO]: Starting key manager in namespace: $1 (pod: $pod_name)"
kubectl -n $1 exec -it $pod_name -- /bin/bash
