# Apache Spark Scala Practice

This repository contains my hands-on exercises and examples while following the 17.5-hour Udemy course **Apache Spark Programming in Scala (2026)**.

I am using this project to deepen my Apache Spark and Scala expertise while continuing to work as a data engineer. It is a small, practical way to stay motivated, build a consistent learning habit, and turn course concepts into runnable code.

## Course topics

The course contains 47 lectures across nine sections (about 17.5 hours total). This repository follows along with the practical programming exercises.

1. **Big Data foundations** — the history and evolution of Hadoop, Big Data, and data lake/lakehouse architecture.
2. **Apache Spark and Databricks** — Spark system architecture, the development platform, and Databricks Cloud.
3. **Development environment** — Databricks account and cluster setup, local IDE setup, Spark Connect, and application deployment.
4. **Spark sessions and project setup** — local and remote Spark sessions plus the project template.
5. **DataFrames and schemas** — structured data, transformations and actions, DataFrame creation, Spark data types, schema-on-read, and type correction.
6. **DataFrame transformations** — column operations and expressions, filtering, deduplication, sorting, limiting, collecting, and unstructured-data transformations, including LLM-assisted transformation.
7. **Working with values** — nulls, numbers, strings, dates, timestamps, and time-zone information.
8. **Aggregations** — standard, grouped, and window aggregations.
9. **Joins and performance** — DataFrame joins, outer joins, column-name ambiguity, join and shuffle internals, join optimisation, and bucket joins.

## Course credit

Huge thanks to [Prashant Kumar Pandey](https://www.udemy.com/user/prashant-kumar-pandey-13/) for creating and teaching this course.

## Requirements

- Scala 2.13
- sbt 1.11+
- A Databricks workspace for examples that use Spark Connect

For Databricks Connect examples, set these environment variables locally. Never commit their values to Git.

```powershell
$env:DATABRICKS_HOST = "https://<your-workspace-url>"
$env:DATABRICKS_TOKEN = "<your-personal-access-token>"
```

## Disclaimer

This is a personal learning repository. The code is written as practice alongside the course; it is not an official course repository or a production-ready application.
