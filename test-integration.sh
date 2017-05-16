#!/bin/bash

source .ci/variables.sh

DIRS=""
for DIR in $SRC;
do
  if ls $DIR/*integration_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

set -e
set +x
go test -tags integration -run Integration -v -race $DIRS
set -x
