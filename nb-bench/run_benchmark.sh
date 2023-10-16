#!/bin/bash

if [ ! -e "library/nb5.jar" ]; then
    cd library 
    curl -OL https://github.com/nosqlbench/nosqlbench/releases/download/5.17.5-preview/nb5.jar
    cd ../
fi

if [ ! -d "testdata" ]; then
    DATASETS="glove-25-angular glove-50-angular glove-100-angular glove-200-angular deep-image-96-angular lastfm-64-dot"
    mkdir -p testdata
    pushd .
    cd testdata

    DATASET=${DATASETS?is required}

    for dataset in ${DATASETS}
    do
     URL="http://ann-benchmarks.com/${dataset}.hdf5"
     curl -OL "${URL}"
    done
    popd
fi

./recall.py
