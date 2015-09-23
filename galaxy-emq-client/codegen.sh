#!/bin/sh
THRIFT_ROOT_PATH=./src/main/thrift
THRIFT_JAVA_PATH=./src/main/java
THRIFT_PYTHON_PATH=../../galaxy-sdk-python
THRIFT_PHP_PATH=../../galaxy-sdk-php
THRIFT_JS_PATH=../../galaxy-sdk-javascript

if [ $# -ne 1 ]
then echo "Please input sdk language!";
    exit;
fi

echo "Generating "$1" thrift files !";

if [ "$1" = "java" ] || [ "$1" = "all" ]
then
    mkdir -p $THRIFT_JAVA_PATH
    # compile thrift files
    for f in Common.thrift Message.thrift Queue.thrift Range.thrift
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
    mkdir -p $THRIFT_JAVA_PATH
    # compile thrift files
    for f in Common.thrift Message.thrift Queue.thrift Range.thrift
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to Python"
        thrift -out ${THRIFT_PYTHON_PATH}/lib -gen py:new_style $f
    done
    # Add utf8 encoding in generated python source
    find ${THRIFT_PYTHON_PATH}/lib/emq/common -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/emq/message -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/emq/queue -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
    find ${THRIFT_PYTHON_PATH}/lib/emq/range -name "*.py" -type f | xargs sed -i -e "1i # encoding: utf-8"
fi

if [ "$1" = "php" ] || [ "$1" = "all" ]
then
    mkdir -p $THRIFT_JAVA_PATH
    # compile thrift files
    for f in Common.thrift Message.thrift Queue.thrift Range.thrift
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to PHP"
        thrift -out ${THRIFT_PHP_PATH}/lib -gen php:autoload $f
    done
fi

if [ "$1" = "javascript" ] || [ "$1" = "all" ]
then
    mkdir -p $THRIFT_JAVA_PATH
    # compile thrift files
    for f in Common.thrift Message.thrift Queue.thrift Range.thrift
    do
        f=${THRIFT_ROOT_PATH}/${f}
        echo "Compiling $f to JavaScript"
        thrift -out ${THRIFT_JS_PATH}/src/emq -gen js:jquery $f
    done
fi