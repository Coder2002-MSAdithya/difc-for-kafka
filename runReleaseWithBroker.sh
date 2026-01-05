#!/bin/bash
set -e
./gradlew releaseTarGz -x test
tar -xzf core/build/distributions/kafka_2.13-4.0.0.tgz -C core/build/distributions/
rm -rf /tmp/kraft-combined-logs
cd core/build/distributions/kafka_2.13-4.0.0/
./runBroker.sh