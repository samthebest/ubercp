#!/bin/bash

set -e

test -f uber-cp.jar || echo "ERROR: uber-cp.jar not built"
test -f uber-cp.jar

java -cp uber-cp.jar ubercp.UberCp $*
