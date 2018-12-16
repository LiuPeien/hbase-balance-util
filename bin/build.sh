#!/bin/sh

PROJECT_HOME=$(cd $(dirname $0)/..;pwd)
CLASS_PATH=${PROJECT_HOME}/classes
PROJECT_JAR_PATH=${PROJECT_HOME}/lib
PROJECT_JAR_NAME=hbase-ops-1.0-SNAPSHOT.jar
SRC_PATH=${PROJECT_HOME}/src/main/java/com/jinhongliu/hbase

rm -rf ${CLASS_PATH}
rm -rf ${PROJECT_JAR_PATH}
mkdir ${CLASS_PATH}
mkdir ${PROJECT_JAR_PATH}

javac -d ${CLASS_PATH} -classpath `hbase classpath` ${SRC_PATH}/balancer/*.java ${SRC_PATH}/keeper/*.java ${SRC_PATH}/util/*.java

cd ${CLASS_PATH}
jar cvf ${PROJECT_JAR_PATH}/${PROJECT_JAR_NAME} *