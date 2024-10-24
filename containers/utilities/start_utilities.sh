#!/bin/bash

if [[ $# -ne 1 ]] ; then
    echo "[ERROR]: Not enough arguments supplied"
    echo "Usage:"
    echo "./start_utilities.sh [namespace]"
    exit 1
fi

namespace=$1
pod_name=$(kubectl get pods -l=app=utilities -n $1 --no-headers | cut -d" " -f1)

echo "[INFO]: Starting utilities in namespace: $1 (pod: $pod_name)"
kubectl -n $1 exec -it $pod_name -- /bin/bash
