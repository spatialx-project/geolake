# GeoLake

**GeoLake** aims at bringing geospatial support to lakehouses.

![geolake-overview](docs/geolake-overview.png)

## GeoLake Architecture

GeoLake can be used to build a lakehouse with geospatial support. It is built on top of [Apache Spark](https://spark.apache.org/) and [Apache Iceberg](https://iceberg.apache.org/).

- **GeoLake Parquet**: A extension to Apache Parquet to support geospatial data types.
- **Spatial Partition**: A spatial partitioning scheme for Apache Iceberg.
- **Geometry Type**: A geometry type for Apache Iceberg.
- **Spark & Sedona**: Integrate with Apache Spark and Apache Sedona seamlessly.

## Spark SQL Examples
```sql
-- Create table with a geometry type, as well as a spatial partition
CREATE TABLE iceberg.geom_table(
    id int,
    geom geometry
) USING ICEBERG PARTITIONED BY (xz2(geo, 7));

-- insert geometry values using WKT
INSERT INTO iceberg.geom_table VALUES
(1, 'POINT(1 2)'),
(2, 'LINESTRING(1 2, 3 4)'),
(3, 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))');

-- query with spatial predicates
SELECT * FROM iceberg.geom_table
WHERE ST_Contains(geom, ST_Point(0.5, 0.5));
```

## Quickstart

Check this repo [docker-spark-geolake](https://github.com/spatialx-project/docker-spark-geolake) for early access, there are some [notebooks](https://github.com/spatialx-project/docker-spark-geolake/tree/main/spark/notebooks) inside.

Source code and documentation will be released soon.

## PVLDB Artifact

We are submitting a paper titled "GeoLake: Bringing Geospatial Support to Lakehouses" to VLDB (Very Large Data Bases), and we have made the experiment-related code, data, and results available at this repository. Specialy, check [parquet-benchmark](https://github.com/spatialx-project/geplake-parquet-benchmark) for Parquet-related experiments(paper's section 7.2), check [serde-benckmark](https://github.com/Kontinuation/play-with-geometry-serde) for Serde-related experiments(paper's section 7.3), check [Partition-Resolution](https://github.com/spatialx-project/docker-spark-geolake/blob/main/spark/notebooks/benchmark-portotaxi.ipynb) for Partition-related experiments(paper's section 7.4), check [end-2-end](https://github.com/spatialx-project/docker-spark-geolake/blob/main/spark/notebooks/benchmark-portotaxi.ipynb) for end-2-end experiments(paper's section 7.5).

It is noteworthy that, for Partition-related experiments and end-2-end experiments, the corresponding repository only contains code for the Portotaxi dataset. For the TIGER2018 and MSBuildings datasets, you only need to modify the logic for reading the dataset in the code.
