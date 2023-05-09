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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGeometryAggregatePushDown extends SparkExtensionsTestBase {
  private Boolean isSparkSessionCatalog;
  private String select =
      "SELECT count(*), count(geom), system.st_minx(geom), system.st_maxx(geom), system.st_miny(geom), system.st_maxy(geom) FROM "
          + tableName;

  public TestGeometryAggregatePushDown(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void removeTables() {
    sql("USE %s", catalogName);
    sql("DROP TABLE IF EXISTS %s", tableName);
    // functions can not be resolved when using SparkSessionCatalog
    // which is a bug to be fixed:
    // https://github.com/apache/iceberg/pull/7153#issuecomment-1483977664
    isSparkSessionCatalog = catalogName.equals("spark_catalog");
  }

  @Test
  public void testDifferentDataTypesAggregatePushDownInPartitionedTable() {
    if (isSparkSessionCatalog) {
      return;
    }
    testDifferentDataTypesAggregatePushDown(true);
  }

  @Test
  public void testDifferentDataTypesAggregatePushDownInNonPartitionedTable() {
    if (isSparkSessionCatalog) {
      return;
    }
    testDifferentDataTypesAggregatePushDown(false);
  }

  private Boolean runExplainSql() {
    List<Object[]> explain = sql("EXPLAIN " + select);
    String explainString = explain.get(0)[0].toString().toLowerCase(Locale.ROOT);
    System.out.println("print explainString: ");
    System.out.println(explainString);

    return explainString.contains("count(geom)")
        && explainString.contains("st_minx(geom)")
        && explainString.contains("st_maxx(geom)")
        && explainString.contains("st_miny(geom)")
        && explainString.contains("st_maxy(geom)")
        && explainString.contains("localtablescan");
  }

  private void selectResult(List<Object[]> expected) {
    List<Object[]> result = sql(select);
    System.out.println("print actual: ");
    result.forEach(
        objects -> {
          for (Object object : objects) {
            System.out.println(object);
          }
        });
    assertEquals(
        "count(*)/count(geom)/st_minx/st_maxx/st_miny/st_maxy push down", expected, result);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void testDifferentDataTypesAggregatePushDown(boolean hasPartitionCol) {
    String createTable;
    if (hasPartitionCol) {
      createTable =
          "CREATE TABLE %s (id LONG, int_data INT, boolean_data BOOLEAN, float_data FLOAT, double_data DOUBLE, "
              + "decimal_data DECIMAL(14, 2), binary_data binary, geom geometry) USING iceberg PARTITIONED BY (id)";
    } else {
      createTable =
          "CREATE TABLE %s (id LONG, int_data INT, boolean_data BOOLEAN, float_data FLOAT, double_data DOUBLE, "
              + "decimal_data DECIMAL(14, 2), binary_data binary, geom geometry) USING iceberg";
    }

    sql(createTable, tableName);
    sql(
        "INSERT INTO TABLE %s VALUES "
            + "(1, null, false, null, null, 11.11, X'1111', IcebergSTGeomFromText('Point(1 2)')),"
            + " (1, null, true, 2.222, 2.222222, 22.22, X'2222', IcebergSTGeomFromText('Point(3 4)')),"
            + " (2, 33, false, 3.333, 3.333333, 33.33, X'3333', IcebergSTGeomFromText('Point(5 6)')),"
            + " (2, 44, true, null, 4.444444, 44.44, X'4444', IcebergSTGeomFromText('Point(7 8)')),"
            + " (3, 55, false, 5.555, 5.555555, 55.55, X'5555', IcebergSTGeomFromText('Point(9 10)')),"
            + " (3, null, true, null, 6.666666, 66.66, null, IcebergSTGeomFromText('Point(11 12)')) ",
        tableName);
    boolean explainContainsPushDownAggregates = runExplainSql();
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {6L, 6L, 1.0, 11.0, 2.0, 12.0});
    selectResult(expected);
  }

  @Test
  public void testHasNull() {
    if (isSparkSessionCatalog) {
      return;
    }
    sql("CREATE TABLE %s (id int, geom geometry) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "INSERT INTO %s VALUES (1, IcebergSTGeomFromText('Point(1 2)')),"
            + "(1, IcebergSTGeomFromText('Point(1 3)')), "
            + "(2, IcebergSTGeomFromText('Point(2 4)')), "
            + "(2, null)",
        tableName);
    Boolean explainContainsPushDownAggregates = runExplainSql();
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {4L, 3L, 1.0, 2.0, 2.0, 4.0});
    selectResult(expected);
  }

  @Test
  public void testHasNullWithoutPushDown() {
    if (isSparkSessionCatalog) {
      return;
    }
    sql("CREATE TABLE %s (id int, geom geometry) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "geom", "none");
    sql(
        "INSERT INTO %s VALUES (1, IcebergSTGeomFromText('Point(1 2)')),"
            + "(1, IcebergSTGeomFromText('Point(1 3)')), "
            + "(2, IcebergSTGeomFromText('Point(4 4)')), "
            + "(2, null)",
        tableName);
    Boolean explainContainsPushDownAggregates = runExplainSql();
    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {4L, 3L, 1.0, 4.0, 2.0, 4.0});
    selectResult(expected);
  }

  @Test
  public void testAllNull() {
    if (isSparkSessionCatalog) {
      return;
    }
    sql("CREATE TABLE %s (id int, geom geometry) USING iceberg PARTITIONED BY (id)", tableName);
    sql("INSERT INTO %s VALUES (1, null), (2, null)", tableName);
    Boolean explainContainsPushDownAggregates = runExplainSql();
    Assert.assertTrue(
        "explain should contain the pushed down aggregates", explainContainsPushDownAggregates);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {2L, 0L, null, null, null, null});
    selectResult(expected);
  }

  @Test
  public void testAllNullWithoutPushDown() {
    if (isSparkSessionCatalog) {
      return;
    }
    sql("CREATE TABLE %s (id int, geom geometry) USING iceberg PARTITIONED BY (id)", tableName);
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "geom", "none");
    sql("INSERT INTO %s VALUES (1, null), (2, null)", tableName);
    Boolean explainContainsPushDownAggregates = runExplainSql();
    Assert.assertFalse(
        "explain should not contain the pushed down aggregates", explainContainsPushDownAggregates);
    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {2L, 0L, null, null, null, null});
    selectResult(expected);
  }
}
