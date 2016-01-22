#!/bin/bash

THRIFT_ROOT_PATH=./src/main/thrift
THRIFT_JAVA_PATH=./src/main/java
THRIFT_PYTHON_PATH=../../galaxy-sdk-python
THRIFT_PHP_PATH=../../galaxy-sdk-php
THRIFT_JS_PATH=../../galaxy-sdk-javascript
THRIFT_GO_PATH=../../galaxy-sdk-go
THRIFT_NODEJS_PATH=../../galaxy-sdk-nodejs

# generate patch number
PATCH_NUM=`git rev-parse HEAD | cut -c1-8`
if [ -z "${PATCH_NUM}" ]; then
  PATCH_NUM="00000000"
fi
find ${THRIFT_ROOT_PATH}/ -name "*.thrift" -type f | xargs sed -i "s/\([0-9]\+\):\s*optional\s*string\s\+patch = '.*',/\1: optional string patch = '${PATCH_NUM}',/g"

THRIFT_FILES=(Common.thrift Errors.thrift Authentication.thrift Table.thrift Admin.thrift SLFile.thrift)

if [ $# -ne 1 ]
then echo "Please input sdk language!";
    exit;
fi

echo "Generating "$1" thrift files !";

if [ "$1" = "java" ] || [ "$1" = "all" ]
then
    mkdir -p $THRIFT_JAVA_PATH
    # compile thrift files
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to Java"
        thrift -out ${THRIFT_JAVA_PATH} -gen java $f
    done
    # Rename java package
    if [ `uname` = "Linux" ]; then
        find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "s/org.apache.thrift/libthrift091/g"
    elif [ `uname` = "Darwin" ]; then
        find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "" "s/org.apache.thrift/libthrift091/g"
    fi
fi

if [ "$1" = "python" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to Python"
        thrift -out ${THRIFT_PYTHON_PATH}/lib -gen py:new_style $f
    done
    # Add utf8 encoding in generated python source
    find ${THRIFT_PYTHON_PATH}/lib/sds/common -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/sds/errors -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/sds/auth -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/sds/admin -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/sds/table -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
fi

if [ "$1" = "php" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to PHP"
        thrift -out ${THRIFT_PHP_PATH}/lib -gen php:autoload $f
    done
fi

if [ "$1" = "javascript" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to JavaScript"
        thrift -out ${THRIFT_JS_PATH}/src/sds -gen js:jquery $f
    done
fi

if [ "$1" = "go" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to Go"
        thrift -out ${THRIFT_GO_PATH} -gen go:package_prefix=github.com/XiaoMi/galaxy-sdk-go/,thrift_import=github.com/XiaoMi/galaxy-sdk-go/thrift $f
    done
fi

if [ "$1" = "nodejs" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to node.js"
        thrift -out ${THRIFT_NODEJS_PATH}/lib/sds -gen js:node $f
    done
	find ${THRIFT_NODEJS_PATH}/lib/sds -name "*.js" -type f | xargs sed -i "s/require('thrift')/require('..\/thrift')/g"
fi