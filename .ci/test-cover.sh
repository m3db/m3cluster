#!/bin/bash
<<<<<<< HEAD

set -e

SOURCE=${1:-.}
TARGET=${2:-profile.cov}
LOG=${3:-test.log}
=======
. "$(dirname $0)/variables.sh"

set -e

TARGET=${1:-profile.cov}
LOG=${2:-test.log}
>>>>>>> c867de7... updating makefile, travis config

rm $TARGET &>/dev/null || true
echo "mode: count" > $TARGET
echo "" > $LOG

DIRS=""
<<<<<<< HEAD
for DIR in $(find $SOURCE -maxdepth 10 -not -path '*/.git*' -not -path '*/.ci*' -not -path '*/_*' -not -path '*/vendor/*' -type d);
=======
for DIR in $SRC;
>>>>>>> c867de7... updating makefile, travis config
do
  if ls $DIR/*_test.go &> /dev/null; then
    DIRS="$DIRS $DIR"
  fi
done

NPROC=$(getconf _NPROCESSORS_ONLN)
<<<<<<< HEAD
go run .ci/gotestcover/gotestcover.go -covermode=set -coverprofile=${TARGET} -v -parallelpackages $NPROC $DIRS | tee $LOG

TEST_EXIT=${PIPESTATUS[0]}

find . | grep \\.tmp | xargs -I{} rm {}
=======
echo "test-cover begin: concurrency $NPROC"
go run .ci/gotestcover/gotestcover.go -race -covermode=atomic -coverprofile=profile.tmp -v -parallelpackages $NPROC $DIRS | tee $LOG

TEST_EXIT=${PIPESTATUS[0]}

cat profile.tmp | grep -v "_mock.go" > $TARGET

find . -not -path '*/vendor/*' | grep \\.tmp$ | xargs -I{} rm {}
>>>>>>> c867de7... updating makefile, travis config
echo "test-cover result: $TEST_EXIT"

exit $TEST_EXIT
