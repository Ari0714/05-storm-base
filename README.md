# 05-storm-base

A Java-based Apache Storm learning project that demonstrates how to build local Storm topologies, process streaming data, and integrate with MySQL for ETL-style data handling.

This repository is mainly a practical demo project for learning Storm concepts such as Spouts, Bolts, grouping, tuple processing, local mode execution, and simple data pipelines.

## Project overview

The project contains several small example topologies under `src/main/java/com/itbys/storm_demo`:

- `demo01`: a minimal topology that emits numbers and prints them
- `demo02_wc`: a word-count pipeline
- `demo03_work`: a multi-source processing example
- `work/demo01_ETLmysql`: ETL-style file-to-MySQL processing
- `work/demo02_conn_region`: IP region matching and longitude/latitude enrichment workflow

These examples are intentionally simple and educational rather than production-ready.

## Tech stack

- Java
- Maven
- Apache Storm 1.1.1
- MySQL Connector/J 5.1.46

## Maven dependencies

The project uses the following dependencies declared in `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.storm</groupId>
    <artifactId>storm-core</artifactId>
    <version>1.1.1</version>
</dependency>

<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```

## Repository structure

```text
05-storm-base/
├── pom.xml
├── README.md
├── input/
├── logs/
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

## Example modules

### demo01

The smallest working Storm topology.

- `_01_spout` emits random integers
- `_02_bolt` prints each value
- `_03_topology` creates a topology and runs it in local mode

This is the basic introduction to a Spout + Bolt topology.

### demo02_wc

A classic word-count streaming example.

- `_01_spout` emits log lines
- `_02_splitBolt` splits text into words
- `_03_countBolt` counts word occurrences with a `HashMap`
- `_04_printBolt` prints `word:count`
- `_99_topology` submits the whole pipeline locally

### demo03_work

A multi-source topology that combines several Spouts and a shared Bolt.

This demonstrates how multiple data sources can feed into one processing stage using `shuffleGrouping`.

### work/demo01_ETLmysql

A file-based ETL example that inserts text file data into MySQL tables.

- `_01_spout` reads `input/ip_area_isp.txt`
- `_02_spout` reads `input/lng_lat.txt`
- `_03_bolt` writes the content into MySQL tables such as `ip_area_isp` and `lng_lat_mapping`

### work/demo02_conn_region

A more realistic analytics-flow example.

- `_01_spout` reads `input/app.log`
- `_02_bolt` parses IP and queries MySQL for region information
- `_03_bolt` looks up longitude/latitude and writes result rows to `phone_ip_lng_lat`

This is the most complete example in the project and shows a typical data-enrichment pipeline.

## Prerequisites

Before running the examples, ensure you have:

- JDK 8 or newer
- Maven
- IntelliJ IDEA or Eclipse
- MySQL server if you are running the database examples

## Build

From the project root, run:

```bash
mvn clean package
```

## Run the examples

The project is intended to be run from an IDE by executing the `main` methods in the topology classes.

Example entries:

- `com.itbys.storm_demo.demo01._03_topology`
- `com.itbys.storm_demo.demo02_wc._99_topology`
- `com.itbys.storm_demo.demo03_work._99_topology`
- `com.itbys.storm_demo.work.demo01_ETLmysql._99_topology`
- `com.itbys.storm_demo.work.demo02_conn_region._99_topology`

## Input data

The current code expects data files inside a project-local `input` directory:

```text
05-storm-base/
├── input/
│   ├── app.log
│   ├── ip_area_isp.txt
│   └── lng_lat.txt
└── src/
```

The code uses paths like:

```java
"input/app.log"
"input/ip_area_isp.txt"
"input/lng_lat.txt"
```

If the files are missing, the Spouts will fail to open them and may throw a `FileNotFoundException`.

## MySQL configuration

The database examples use hard-coded JDBC settings such as:

```java
jdbc:mysql://hdp103:3306/test
```

with:

- username: `root`
- password: `111111`

These values are example-specific and should be adjusted to your local MySQL setup before executing the ETL examples.

## Notes

- This project is for learning and experimentation.
- The code is intentionally simple and occasionally rough.
- Many examples use static file paths and hard-coded database settings.
- Some older files may still contain legacy absolute Windows paths, but the newer examples prefer the `input/` folder.

## License

No explicit license file is included in this repository. If you want to reuse or redistribute this project, confirm the licensing terms with the repository owner before doing so.
