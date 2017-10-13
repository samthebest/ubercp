#!/bin/bash

set -e

sbt assembly

mv target/scala-2.11/uber-cp-assembly.jar uber-cp.jar
