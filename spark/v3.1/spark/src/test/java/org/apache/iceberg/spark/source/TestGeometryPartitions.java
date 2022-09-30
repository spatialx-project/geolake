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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.iceberg.udt.UDTRegistration;
import org.apache.spark.sql.types.StructType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestGeometryPartitions extends SparkTestBase {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "geo", Types.GeometryType.get()));

  @Parameterized.Parameters(name = "tableName = {0}, partitionSpec = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"geo_table_without_partition", null},
      {"geo_table_with_xz2_partition", PartitionSpec.builderFor(SCHEMA).xz2("geo", 2).build()},
      {
        "geo_table_with_void_geo_partition",
        PartitionSpec.builderFor(SCHEMA).alwaysNull("geo").build()
      }
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private final PartitionSpec partitionSpec;
  private final String tableName;

  public TestGeometryPartitions(String tableName, PartitionSpec partitionSpec) {
    this.partitionSpec = partitionSpec;
    this.tableName = tableName;
    UDTRegistration.registerTypes();
  }

  @Test
  public void testGeometryPartition() throws IOException {
    File location = temp.newFolder(tableName);
    Table table = TABLES.create(SCHEMA, partitionSpec, location.toString());
    Dataset<Row> tableDf = spark.read().format("iceberg").load(table.location());
    StructType schema = tableDf.schema();

    // Generic random data
    Random random = new Random();
    List<Object[]> expectedRows = Lists.newArrayList();
    Transform<Object, Long> xz2 = Transforms.xz2(2);
    for (int k = 0; k < 100; k++) {
      Object[] values = new Object[3];
      values[0] = k;
      values[1] = String.format("str_%d", k);
      double lon = random.nextDouble() * 200 - 100;
      double lat = random.nextDouble() * 160 - 80;
      values[2] = TypeUtil.GeometryUtils.wkt2geometry(String.format("POINT (%f %f)", lon, lat));
      expectedRows.add(values);
    }

    // rows should be clustered by xz2(geo) for writing to partitioned tables
    List<Row> rows =
        expectedRows.stream()
            .sorted(
                (row0, row1) -> {
                  Long row0Xz2 = xz2.apply(row0[2]);
                  Long row1Xz2 = xz2.apply(row1[2]);
                  return row0Xz2.compareTo(row1Xz2);
                })
            .map(GenericRow::new)
            .collect(Collectors.toList());

    Dataset<Row> geomDf = spark.createDataFrame(rows, schema);
    geomDf.write().format("iceberg").mode("overwrite").save(table.location());
    List<Object[]> actualRows =
        rowsToJava(
            spark.read().format("iceberg").load(table.location()).orderBy("id").collectAsList());
    assertEquals(
        "Records read from partitioned table should match with records written",
        expectedRows,
        actualRows);
  }
}
