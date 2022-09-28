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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.TypeUtil.GeometryUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class TestTableWithGeometryColumn extends SparkExtensionsTestBase {
  public TestTableWithGeometryColumn(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private final Row[] rows = new Row[100];
  private final List<Object[]> expected = Lists.newArrayList();
  private final Geometry queryWindow =
      TypeUtil.GeometryUtils.wkt2geometry("POLYGON ((40 60, 40 80, 60 80, 60 60, 40 60))");

  @Test
  public void testGeometryTable() throws NoSuchTableException {
    prepareData();

    String[] geometryEncodings = {"wkb", "wkb-bbox", "nested-list"};
    String[] geometryPartition = {" PARTITIONED BY (xz2(2, geo)) ", ""};
    String[] vectorizationSetting = {"true", "false"};
    for (String geometryEncoding : geometryEncodings) {
      for (String partition : geometryPartition) {
        for (String vectorizationEnabled : vectorizationSetting) {
          testGeometryTable(partition, geometryEncoding, vectorizationEnabled);
        }
      }
    }
  }

  private void prepareData() {
    Random random = new Random();
    for (int k = 0; k < rows.length; k++) {
      Object[] values = new Object[3];
      values[0] = (long) k;
      values[1] = String.format("str_%d", k);
      double lon = random.nextDouble() * 200 - 100;
      double lat = random.nextDouble() * 160 - 80;
      Geometry geom = GeometryUtils.wkt2geometry(String.format("POINT (%f %f)", lon, lat));
      values[2] = geom;
      rows[k] = new GenericRow(values);
      if (geom.within(queryWindow)) {
        expected.add(values);
      }
    }
  }

  private void testGeometryTable(
      String partition, String geometryEncoding, String vectorizationEnabled)
      throws NoSuchTableException {
    String hint =
        String.format(
            "(geometryEncoding: %s; partition:%s; vectorizationEnabled: %s)",
            geometryEncoding, partition, vectorizationEnabled);
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id bigint, data string, geo geometry) USING iceberg "
            + partition
            + "TBLPROPERTIES ('read.parquet.vectorization.enabled' = '%s', "
            + "'write.parquet.geometry.encoding' = '%s')",
        tableName,
        vectorizationEnabled,
        geometryEncoding);
    Dataset<Row> tableDf = spark.table(tableName);
    StructType schema = tableDf.schema();
    Dataset<Row> geomDf = spark.createDataFrame(Arrays.asList(rows), schema);
    geomDf.writeTo(tableName).overwritePartitions();
    Assert.assertEquals(
        hint + " Should have inserted 100 rows",
        100L,
        scalarSql("SELECT COUNT(*) FROM %s", tableName));
    Assert.assertEquals(
        hint + " Row should have correct geo value",
        rows[5].get(2).toString(),
        scalarSql("SELECT geo FROM %s WHERE id = 5", tableName).toString());
    List<Object[]> actual =
        sql(
            "SELECT * FROM %s WHERE IcebergSTCoveredBy(geo, IcebergSTGeomFromText('%s')) ORDER BY id",
            tableName, queryWindow.toString());
    assertEquals(hint + " Spatial query should work as expected", expected, actual);
  }
}
