#!/bin/sh
THRIFT_ROOT_PATH=./src/main/thrift
THRIFT_JAVA_PATH=./src/main/java

mkdir -p $THRIFT_JAVA_PATH
# compile thrift files
for f in Common.thrift Message.thrift Queue.thrift Range.thrift
do
  f=${THRIFT_ROOT_PATH}/${f}
  echo "Compiling $f"
  echo "Compiling $f to Java"
  thrift -out ${THRIFT_JAVA_PATH} -gen java $f
done

# Rename java package
if [ `uname` = "Linux" ]; then
  find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "s/org.apache.thrift/libthrift091/g"
elif [ `uname` = "Darwin" ]; then
  find ${THRIFT_JAVA_PATH} -name "*.java" -type f | xargs sed -i "" "s/org.apache.thrift/libthrift091/g"
fi

