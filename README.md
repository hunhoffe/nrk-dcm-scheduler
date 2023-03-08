[![License](https://img.shields.io/badge/License-BSD%202--Clause-green.svg)](https://opensource.org/licenses/BSD-2-Clause)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)
![Build](https://github.com/hunhoffe/nrk-dcm-scheduler/actions/workflows/ci.yaml/badge.svg)
![GitHub release (latest by date)](https://img.shields.io/github/v/release/hunhoffe/nrk-dcm-scheduler)

# Rackscale NrOS DCM Scheduler

## About

This project uses [Declarative Cluster Management (DCM)](https://github.com/vmware/declarative-cluster-management) to schedule cores and memslices (fixed-size chunks of memory) to nodes within a rackscale cluster.

This project contains a Simulator and SimulatorRunner, which can be used to benchmark DCM over simulated cluster conditions. This project also contains a Scheduler and SchedulerRunner which can interface with NRK using RPCs and a UDP connection to accept resource requests and return assignments.

## Dependencies

If you need to setup a development environment, proceed to the development section. If you are a project user, please setup your environment as below. There are two dependencies needed to run this project:
* **Java**: Install Java 16 SDK. I belive this project also works with Java 18 but DCM is
not tested with Java 18, so best to be safe with Java 16.
* **Minizinc**: DCM needs the minizinc solver installed. Follow the links on the [DCM README](https://github.com/hunhoffe/declarative-cluster-management) 
to install minizinc, or the command below. The GitHub CI runner uses Minizinc 2.3.2.

Install the runtime dependencies with the command below:
```
sudo apt install -y openjdk-16-jre openjdk-16-jdk minizinc
```

These commands were tested on Ubuntu 20.04.

## How to Run

### Run the Scheduler:
Download a [release jar](https://github.com/hunhoffe/nrk-dcm-scheduler/releases).

```bash
java -jar scheduler-<version>-jar-with-dependencies.jar  [OPTIONS]
```
Run with a ```-h``` to see parameters and usage instructions.

### Run a Simulation:
Run the jar with:
```bash
java -Dlog4j.configurationFile=src/main/resources/log4j2.xml -cp scheduler-<version>-jar-with-dependencies.jar com.vmware.bespin.scheduler.DCMSimulation [OPTIONS]
```
Run with a ```-h``` to see usage message.

## Development

### Build
Create runnable jars (packaged with and without dependencies) with:
```bash
mvn clean package
```
You don't always need the clean, but if you update the database schema it is needed. the produced jars will be in the ```target``` directory. 

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
