/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

class SparkCopyOnWriteScan extends SparkPartitioningAwareScan<FileScanTask>
    implements SupportsRuntimeFiltering {

  private final Snapshot snapshot;
  private Set<String> filteredLocations = null;

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {
    this(spark, table, null, null, readConf, expectedSchema, filters);
  }

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      BatchScan scan,
      Snapshot snapshot,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {
    this(
        JavaSparkContext.fromSparkContext(spark.sparkContext()),
        table,
        scan,
        snapshot,
        readConf,
        expectedSchema,
        filters);
  }

  SparkCopyOnWriteScan(
      JavaSparkContext sparkContext,
      Table table,
      BatchScan scan,
      Snapshot snapshot,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    super(sparkContext, table, scan, readConf, expectedSchema, filters);

    this.snapshot = snapshot;

    if (scan == null) {
      this.filteredLocations = Collections.emptySet();
    }
  }

  public SparkScan withExpressionsInternal(List<Expression> newFilterExpressions) {
    BatchScan newScan = (BatchScan) this.scan();
    for (Expression expr : newFilterExpressions) {
      newScan = newScan.filter(expr);
    }
    return new SparkCopyOnWriteScan(
        this.sparkContext(),
        this.table(),
        newScan,
        this.snapshot,
        this.readConf(),
        this.expectedSchema(),
        newFilterExpressions);
  }

  Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  @Override
  protected Class<FileScanTask> taskJavaClass() {
    return FileScanTask.class;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(snapshot);
  }

  public NamedReference[] filterAttributes() {
    NamedReference file = Expressions.column(MetadataColumns.FILE_PATH.name());
    return new NamedReference[] {file};
  }

  @Override
  public void filter(Filter[] filters) {
    Preconditions.checkState(
        Objects.equals(snapshotId(), currentSnapshotId()),
        "Runtime file filtering is not possible: the table has been concurrently modified. "
            + "Row-level operation scan snapshot ID: %s, current table snapshot ID: %s. "
            + "If multiple threads modify the table, use independent Spark sessions in each thread.",
        snapshotId(),
        currentSnapshotId());

    for (Filter filter : filters) {
      // Spark can only pass In filters at the moment
      if (filter instanceof In
          && ((In) filter).attribute().equalsIgnoreCase(MetadataColumns.FILE_PATH.name())) {
        In in = (In) filter;

        Set<String> fileLocations = Sets.newHashSet();
        for (Object value : in.values()) {
          fileLocations.add((String) value);
        }

        // Spark may call this multiple times for UPDATEs with subqueries
        // as such cases are rewritten using UNION and the same scan on both sides
        // so filter files only if it is beneficial
        if (filteredLocations == null || fileLocations.size() < filteredLocations.size()) {
          this.filteredLocations = fileLocations;
          List<FileScanTask> filteredTasks =
              tasks().stream()
                  .filter(file -> fileLocations.contains(file.file().path().toString()))
                  .collect(Collectors.toList());
          resetTasks(filteredTasks);
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkCopyOnWriteScan that = (SparkCopyOnWriteScan) o;
    return table().name().equals(that.table().name())
        && readSchema().equals(that.readSchema())
        && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString())
        && Objects.equals(snapshotId(), that.snapshotId())
        && Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        readSchema(),
        filterExpressions().toString(),
        snapshotId(),
        filteredLocations);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergCopyOnWriteScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }

  private Long currentSnapshotId() {
    Snapshot currentSnapshot = table().currentSnapshot();
    return currentSnapshot != null ? currentSnapshot.snapshotId() : null;
  }
}
