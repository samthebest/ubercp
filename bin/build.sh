#!/bin/bash

set -e

sbt assembly

mv target/scala-2.11/uber-cp-assembly-0.1-SNAPSHOT.jar uber-cp.jar
