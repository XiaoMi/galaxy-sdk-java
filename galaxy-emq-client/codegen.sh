#!/bin/bash
THRIFT_ROOT_PATH=./src/main/thrift
THRIFT_JAVA_PATH=./src/main/java
THRIFT_PYTHON_PATH=../../galaxy-sdk-python
THRIFT_PHP_PATH=../../galaxy-sdk-php
THRIFT_JS_PATH=../../galaxy-sdk-javascript
THRIFT_NODE_PATH=../../galaxy-sdk-nodejs
THRIFT_GO_PATH=../../galaxy-sdk-go

THRIFT_FILES=(Common.thrift Range.thrift Constants.thrift Message.thrift Queue.thrift Statistics.thrift)

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
        dir=`echo $f | awk -F '.' '{print $1}' | tr A-Z a-z`
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to Python"
        thrift -out ${THRIFT_PYTHON_PATH}/lib -gen py:new_style $f
        # Add utf8 encoding in generated python source
        find ${THRIFT_PYTHON_PATH}/lib/emq/${dir} -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    done
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
        thrift -out ${THRIFT_JS_PATH}/src/emq -gen js:jquery $f
    done
fi

if [ "$1" = "nodejs" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to JavaScript"
        thrift -out ${THRIFT_NODE_PATH}/lib/emq -gen js:node $f
    done
    find ${THRIFT_NODE_PATH}/lib/emq -name "*.js" -type f | xargs sed -i "s/require('thrift')/require('..\/thrift')/g"
fi

if [ "$1" = "go" ] || [ "$1" = "all" ]
then
    for f in ${THRIFT_FILES[@]}
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to go"
        thrift -out ${THRIFT_GO_PATH} -gen go:package_prefix=github.com/XiaoMi/galaxy-sdk-go/,thrift_import=github.com/XiaoMi/galaxy-sdk-go/thrift $f
    done
fi


