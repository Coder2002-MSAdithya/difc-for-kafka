#!/bin/bash
set -e
java -cp "core/build/distributions/kafka_2.13-4.0.0/libs/*" examples/src/main/java/kafka/examples/DIFCTagProducerExample.java