#!/bin/bash
. common-classpath.sh
CLASSPATH="$CLASSPATH:/home/hgraph/clojure-1.5.1.jar"
export CLASSPATH
java -cp $CLASSPATH clojure.main
