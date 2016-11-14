#!/bin/bash

#!/bin/sh
THRIFT_ROOT_PATH=./src/main/thrift
THRIFT_JAVA_PATH=./src/main/java

# compile thrift files
for f in lcs.thrift
do
  f=${THRIFT_ROOT_PATH}/${f}
  echo "Compiling $f"
  # echo "Compiling $f to Java"
  thrift -out ${THRIFT_JAVA_PATH} -gen java $f
  if [ $? -ne 0 ]; then
    echo "generate java code for $f failed"
    exit
  fi
done

# Rename java package
if [ `uname` = "Linux" ]; then
  find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "s/org.apache.thrift/libthrift091/g"
elif [ `uname` = "Darwin" ]; then
  find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "" "s/org.apache.thrift/libthrift091/g"
fi

