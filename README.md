# 05-storm-base

This project is a small Apache Storm learning/demo project built with Java and Maven. It contains several local-topology examples that show how to create Spouts, connect Bolts, and process streaming data in a Storm pipeline.

The code is organized as a set of beginner-friendly examples for understanding stream processing concepts such as data generation, splitting, counting, filtering, and basic ETL-style processing.

## Overview

The project uses Apache Storm in local mode with a `TopologyBuilder` and `LocalCluster` to run topologies directly on a single machine for testing and learning.

The main packages under `src/main/java/com/itbys/storm_demo` include:

- `demo01`: a basic stream example where a Spout emits random numbers and a Bolt receives them.
- `demo02_wc`: a word-count topology made of several stages: Spout -> Split Bolt -> Count Bolt -> Print Bolt.
- `demo03_work`: a multi-source topology where several Spouts send data into a shared Bolt.
- `work/demo01_ETLmysql`: a data-processing example that reads input data and performs ETL-like processing using MySQL connectivity.
- `work/demo02_conn_region`: a region/correlation style data-processing pipeline.

## Project structure

```text
05-storm-base/
├── pom.xml
├── src/
│   ├── main/
│   │   └── java/
│   │       └── com/
│   │           └── itbys/
│   │               └── storm_demo/
│   │                   ├── demo01/
│   │                   ├── demo02_wc/
│   │                   ├── demo03_work/
│   │                   └── work/
│   │                       ├── demo01_ETLmysql/
│   │                       └── demo02_conn_region/
│   └── test/
│       └── java/
│           └── Test01.java
└── target/
```

## Prerequisites

Before running the demos, make sure you have:

- JDK 8 or above
- Maven
- An IDE such as IntelliJ IDEA or Eclipse (recommended for running the topology main classes)
- MySQL driver support for the data-processing examples

## Dependencies

The project uses Maven and includes:

- `org.apache.storm:storm-core:1.1.1`
- `mysql:mysql-connector-java:5.1.46`

These dependencies are declared in `pom.xml`.

## Quick start

1. Clone the project:

```bash
git clone https://github.com/Ari0714/05-storm-base.git
cd 05-storm-base
```

2. Build the project:

```bash
mvn clean package
```

3. Run any topology class that contains a `main` method from your IDE.

Examples include:

- `com.itbys.storm_demo.demo01._03_topology`
- `com.itbys.storm_demo.demo02_wc._99_topology`
- `com.itbys.storm_demo.demo03_work._99_topology`
- `com.itbys.storm_demo.work.demo01_ETLmysql._99_topology`
- `com.itbys.storm_demo.work.demo02_conn_region._99_topology`

## Example behavior

### 1. Basic demo

`demo01` creates a simple Storm topology with a Spout that emits random numbers and a Bolt that processes them.

### 2. Word count demo

`demo02_wc` demonstrates a classic streaming pipeline:

- Spout emits log lines
- Split Bolt breaks text into words
- Count Bolt aggregates counts
- Print Bolt outputs the final results

### 3. ETL-style demo

`work/demo01_ETLmysql` reads data from a file, passes it through a stream processing flow, and interacts with MySQL for database-related logic.

## Important note about file paths

Some sample Spouts use hard-coded local Windows file paths such as:

```java
C:\Users\Administrator\Desktop\02storm\原数据\...
```

These paths are environment-specific and may not exist on your machine. If you run the file-based examples, update those paths to match your local dataset location before executing the topologies.

## Learning purpose

This repository is intended as a practical Storm sandbox for learning:

- how topologies are built
- how Spouts and Bolts communicate
- how streams are split and aggregated
- how local-mode Storm applications are tested and debugged

## License

This project does not currently declare an explicit license file. Please check with the repository owner before using it in production or redistribution.
