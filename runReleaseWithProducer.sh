#!/bin/bash
./gradlew releaseTarGz -x test
tar -xzf core/build/distributions/kafka_2.13-4.0.0.tgz