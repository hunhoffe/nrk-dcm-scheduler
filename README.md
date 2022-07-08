[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

# Rackscale NrOS DCM Scheduler

## About

This project uses [Declarative Cluster Management (or DCM)](https://github.com/vmware/declarative-cluster-management) to schedule cores and memslices (fixed-size chunks of memory) to nodes within a rackscale cluster.

This project contains a Simulator and Simulator Runner, which can be used to benchmark DCM over simulated cluster conditions. This project also contains a Scheduler and Scheduler Runner which can interface with NRK using RPCs and a UDP connection to accept resource requests and return assignments.

## Dependencies

Install Java 16 SDK and maven 3.8.4. I think it also works with Java 18 but DCM is
not tested with Java 18, so best to be safe with Java 16.
```
sudo apt install openjdk-16-jre openjdk-16-jdk
sudo apt install maven
```

The default maven version for Ubuntu did NOT work when I tried it, so I used some
directions online to manually install 3.8.4 (the symbolic links and maven download link
didn't work in [this](https://phoenixnap.com/kb/install-maven-on-ubuntu) tutorial, 
but all the rest of the instructions did).

### DCM Build
We use a custom version of DCM, for efficiency purposes, found [here](https://github.com/hunhoffe/declarative-cluster-management)
You'll need to build and install the jar into your local maven repository, rather than using the general DCM version.

Follow the links on the [DCM README](https://github.com/hunhoffe/declarative-cluster-management) 
to install minizinc.
```
sudo apt install minizinc
```

Then clone the DCM repo. Build and test to make sure it's functioning correctly:
```
git clone git@github.com:hunhoffe/declarative-cluster-management.git dcm
cd dcm
git checkout --track origin/capacity_function
./gradlew build -x test
```

Then, push the jar to your local maven repository:
```
./gradlew publishToMavenLocal -x sign
```

## How to Build and Run

### Build
Create a runnable jar with:
```bash
mvn clean compile assembly:single
```
You don't always need the clean, but if you update the database schema it is needed.

### Run the Scheduler:
Run the jar with:
```bash
java -jar target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [OPTIONS]
```
Run with a ```-h``` to see parameters and usage instructions.

### Run a Simulation:
Run the jar with:
```bash
java -Dlog4j.configurationFile=src/main/resources/log4j2.xml -cp target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar com.vmware.bespin.scheduler.SimulationRunner [OPTIONS]
```
Run with a ```-h``` to see usage message.

### Test
```bash
mvn test
```

### Generating Java Classes for SQL Tables

This normally happens during the build command above, but if you want to run it
separately, the command is:
```bash
mvn jooq-codegen:generate
```
