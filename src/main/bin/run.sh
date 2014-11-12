#!/bin/sh
print_usage ()
{
  echo "Usage: sh run.sh COMMAND"
  echo "where COMMAND is one of the follows:"
  echo "son -i <input path> -o <output path> Mine frequent item sets using SON algorithm"
  exit 1
}

if [ $# = 0 ] || [ $1 = "help" ]; then
  print_usage
fi

COMMAND=$1
shift

if [ "$JAVA_HOME" = "" ]; then
  echo "Error: JAVA_HOME is not set."
  exit 1
fi


JAVA=$JAVA_HOME/bin/java
HEAP_OPTS="-Xmx1000m -XX:PermSize=128m -XX:MaxPermSize=512m"

CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
CLASSPATH=${CLASSPATH}:conf
CLASSPATH=${CLASSPATH}:`ls |grep jar|grep bin`

for f in lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

params=$@

if [ "$COMMAND" = "son" ]; then
  CLASS=ca.sfu.dataming.mr.SON
else
  CLASS=$COMMAND
fi


"$JAVA" -Djava.awt.headless=true $HEAP_OPTS -classpath "$CLASSPATH" $CLASS $params
