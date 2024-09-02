# App

This project contains a maven application with [AWS Java SDK 2.x](https://github.com/aws/aws-sdk-java-v2) dependencies.

## Prerequisites
- Java 1.8+
- Apache Maven
- GraalVM Native Image (optional)

## Development

Below is the structure of the generated project.

```
.
├── pom.xml
├── README.md
├── src
│ └── main
│     ├── java
│     │ └── com
│     │     └── amazonaws
│     │         └── UploadObjectS3Demo.java
│     └── resources
│         └── simplelogger.properties
```

#### Building the project
```
mvn clean package
```

#### Run the application
```
mvn exec:java -Dexec.mainClass="com.amazonaws.UploadObjectS3Demo"
```
