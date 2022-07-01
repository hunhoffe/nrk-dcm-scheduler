#!/bin/bash

JAVA_VERSION="16.0.1"
MAVEN_VERSION="3.8.6"

# Check java version
if [[ $(java -version 2>&1 | grep $JAVA_VERSION) ]]; then
	echo "Java version is good."
else
	echo "ERROR: Java version is not $JAVA_VERSION. Please install/configure java to use $JAVA_VERSION"
	exit -1
fi

# Check maven version
if [[ $(mvn -version 2>&1 | grep $MAVEN_VERSION) ]]; then
	echo "Maven version is good."
else
	echo "ERROR: Maven version is not $MAVEN_VERSION. Please install/configure java to use $MAVEN_VERSION"
	exit -1
fi

# Check for github ssh key
if ssh -o "StrictHostKeyChecking no" -T git@github.com 2>&1 | grep -q "You've successfully authenticated"; then
	echo "Verified SSH access to git."
else
	echo "ERROR: Please initialized your github ssh key before proceeding."
	exit -1
fi

# Clone, build, and install custom DCM package
sudo apt install minizinc
git clone git@github.com:hunhoffe/declarative-cluster-management.git dcm
cd dcm
git checkout --track origin/capacity_function
./gradlew build -x test
./gradlew publishToMavenLocal -x sign

# Build the nrk-dcm-scheduler
cd ..
mvn clean compile assembly:single
