#!/bin/bash

cd target 

mkdir -p test-run
cd test-run

tar zxf ../hadoop-unit-standalone*.tar.gz
cd hadoop-unit-standalone*

./bin/hadoop-unit-standalone console 

