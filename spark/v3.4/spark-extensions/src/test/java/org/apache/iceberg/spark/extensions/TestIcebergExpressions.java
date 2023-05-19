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

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Test;

public class TestIcebergExpressions extends SparkExtensionsTestBase {

  public TestIcebergExpressions(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testXZ2Expressions() {
    sql("CREATE TABLE %s (id INT, geo geometry) USING iceberg", tableName);
    sql(
        "INSERT INTO %s VALUES (1, IcebergSTGeomFromText('POINT (-50 -50)')),"
            + "(2, IcebergSTGeomFromText('POINT (50 -50)')),"
            + "(3, IcebergSTGeomFromText('POINT (-50 50)')),"
            + "(4, IcebergSTGeomFromText('POINT (50 50)'))",
        tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L), row(2L), row(3L), row(4L)),
        sql(String.format("SELECT IcebergXZ2(geo, 1) FROM %s ORDER BY id", tableName)));
  }
}
