#!/usr/bin/env bash
MAVEN_OPTS="-Xmx6g -Xms2g -Xss4m -Xverify:none -XX:+UseG1GC -XX:+AggressiveOpts" mvn exec:java
