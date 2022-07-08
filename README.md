[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
![Build](https://github.com/hunhoffe/nrk-dcm-scheduler/actions/workflows/ci.yaml/badge.svg)

# Rackscale NrOS DCM Scheduler

## About

This project uses [Declarative Cluster Management (DCM)](https://github.com/vmware/declarative-cluster-management) to schedule cores and memslices (fixed-size chunks of memory) to nodes within a rackscale cluster.

This project contains a Simulator and SimulatorRunner, which can be used to benchmark DCM over simulated cluster conditions. This project also contains a Scheduler and SchedulerRunner which can interface with NRK using RPCs and a UDP connection to accept resource requests and return assignments.

## Dependencies

Install Java 16 SDK and maven 3.8.4. I think it also works with Java 18 but DCM is
not tested with Java 18, so best to be safe with Java 16.
```
sudo apt install openjdk-16-jre openjdk-16-jdk
```

The default maven version for Ubuntu did NOT work when I tried it, so use the ```maven_install.sh``` script.

DCM needs the minizinc solver installed. Follow the links on the [DCM README](https://github.com/hunhoffe/declarative-cluster-management) 
to install minizinc.
```
sudo apt install minizinc
```

The GitHub CI runner uses Minizinc 2.3.2.

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
