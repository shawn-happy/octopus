#!/bin/bash

DIR=$(cd `dirname $0`; pwd)
PKG_DIR=$DIR/spark-operators

set -ex

echo "clean package directory if exists"
if [ -d "$PKG_DIR" ]; then
    rm -rf "$PKG_DIR"
fi

echo "creating package directory"
mkdir -p "$PKG_DIR"
git rev-parse HEAD > "$PKG_DIR"/version

echo "copy jar files"
ls "$DIR"/../build/libs/*.jar | grep -v sources | xargs -I{} cp {} "$PKG_DIR"
