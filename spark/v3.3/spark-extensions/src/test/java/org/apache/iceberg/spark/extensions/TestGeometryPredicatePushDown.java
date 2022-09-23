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
package org.apache.iceberg.spark.extensions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestGeometryPredicatePushDown extends SparkExtensionsTestBase {

  public TestGeometryPredicatePushDown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private Dataset<Row> prepareTestData(StructType schema, int numRows) {
    Row[] rows = new Row[numRows];
    Random random = new Random();
    for (int k = 0; k < rows.length; k++) {
      Object[] values = new Object[3];
      values[0] = k;
      values[1] = String.format("str_%d", k);
      double lon = random.nextDouble() * 200 - 100;
      double lat = random.nextDouble() * 160 - 80;
      values[2] = TypeUtil.GeometryUtils.wkt2geometry(String.format("POINT (%f %f)", lon, lat));
      rows[k] = new GenericRow(values);
    }
    return spark.createDataFrame(Arrays.asList(rows), schema);
  }

  @Test
  public void testGeometryPredicatePushDown() throws NoSuchTableException {
    // Create non-partitioned table and run queries on it.
    sql("CREATE TABLE %s (id INT, data STRING, geo GEOMETRY) USING iceberg", tableName);
    StructType schema = spark.table(tableName).schema();
    Dataset<Row> dfTest = prepareTestData(schema, 1000);
    dfTest.writeTo(tableName).overwritePartitions();
    String testSql =
        "SELECT * FROM "
            + tableName
            + " WHERE data > 'str_5' AND "
            + "IcebergSTContains(IcebergSTGeomFromText('POLYGON((0 0, 80 0, 80 80, 0 80, 0 0))'), geo) "
            + "ORDER BY id";
    List<Object[]> expectedRows = sql(testSql);

    // Run the same query on the same data with different partition specs.
    String[] partitions = {
      "PARTITIONED BY (truncate(data, 5))",
      "PARTITIONED BY (xz2(geo, 3))",
      "PARTITIONED BY (truncate(data, 5), xz2(geo, 3))",
      "PARTITIONED BY (xz2(geo, 3), truncate(data, 5))"
    };
    Pattern executedPlanPattern = Pattern.compile(".*BatchScan.*st_within\\(geo.*");
    for (String partition : partitions) {
      sql("DROP TABLE IF EXISTS %s", tableName);
      sql(
          "CREATE TABLE %s (id INT, data STRING, geo GEOMETRY) USING iceberg %s",
          tableName, partition);
      dfTest.writeTo(tableName).overwritePartitions();
      String executedPlan = spark.sql(testSql).queryExecution().executedPlan().toString();
      Assert.assertTrue(executedPlanPattern.matcher(executedPlan).find());
      List<Object[]> queryResult = sql(testSql);
      assertEquals("Should have expected rows", expectedRows, queryResult);
    }
  }
}
