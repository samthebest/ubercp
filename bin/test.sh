#!/bin/bash

# greps are Hack-around, because https://stackoverflow.com/a/27360000/1586965 doesn't work anymore
sbt it:test 2>&1 \
  | grep --line-buffered -v " INFO " \
  | grep --line-buffered -v "{" \
  | grep --line-buffered -v "}" \
  | grep --line-buffered -v "\" : " \
  | grep --line-buffered -v "Parquet message type" \
  | grep --line-buffered -v "  optional "
