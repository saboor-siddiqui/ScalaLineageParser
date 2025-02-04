# Scala Lineage Parser

A static analysis tool that generates call graphs for Scala code, with special support for Spark DataFrame operations.

## Overview

This tool analyzes Scala source code and generates a DOT format call graph showing:
- Method call relationships
- Spark DataFrame operation chains
- SparkSession operation sequences


## Prerequisites

- Java 8 or higher
- Maven
- Scala 2.12.x

## Building

```bash
mvn clean package
```

## Features
- Tracks method calls between objects
- Analyzes DataFrame operation chains
- Follows Spark session operations
- Generates DOT format output for visualization
- Thread-safe graph construction

For a Spark DataFrame operation like:

```scala
df.select("col1", "col2")
  .where("col1 > 0")
  .groupBy("col2")
  .agg(Map("col1" -> "sum"))
```
The tool will generate a call graph showing the operation sequence:

```scala
df.select -> df.where -> df.groupBy -> df.agg
```