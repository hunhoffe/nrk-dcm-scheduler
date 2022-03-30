# DCM Controller Scheduler

## Build

Create a runnable jar with:
```bash
mvn clean compile assembly:single
```

## Run

Run the jar with:
```bash
java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [OPTIONS]
```

## Generating Java Classes for SQL Tables

This normally happens during the build command above, but if you want to run it
separately, the command is:
```bash
mvn jooq-codegen:generate
```
