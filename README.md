# Controller Scheduler

## Dependencies

Install Java 16 SDK and maven. I think it also works with Java 18 but DCM is
not tested with Java 18, so best to be safe with Java 16.

### DCM Build
We use the newest version of DCM, so you'll need to build and 
install the jar into your local maven repository.

Follow the links on the [DCM README](https://github.com/vmware/declarative-cluster-management) 
to install minizinc.

Then clone the DCM repo. Build and test to make sure it's functioning correctly:
```
./gradlew build -x test
```
Then, push the jar to your local maven repository:
```
./gradlew publishToMavenLocal -x sign
```

## Controller

### Build
Create a runnable jar with:
```bash
mvn clean compile assembly:single
```
You don't technically always need the clean, but if you update the
database schema it is needed.

### Run
Run the jar with:
```bash
java -jar -Dlog4j.configurationFile=src/main/resources/log4j2.xml target/scheduler-1.0-SNAPSHOT-jar-with-dependencies.jar [OPTIONS]
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
