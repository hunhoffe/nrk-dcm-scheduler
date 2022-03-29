# DCM Controller Scheduler

## Build

Generate Java helper classes by on the bespin_schema.sql file:
```bash
mvn jooq-codegen:generate
```

Build with:
```bash
mvn install
```

## Run
Run with:
```bash
mvn exec:java
```

## Create a Runnable Jar with:
```bash
mvn clean compile assembly:single
```
