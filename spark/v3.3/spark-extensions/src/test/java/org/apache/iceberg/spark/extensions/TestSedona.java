/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.extensions;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.sedona.UDFRegistration;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.apache.iceberg.types.TypeUtil.GeometryUtils.wkt2geometry;

public class TestSedona extends SparkExtensionsTestBase {

	public TestSedona(String catalogName, String implementation, Map<String, String> config) {
		super(catalogName, implementation, config);
	}

	@Test
	public void testSedona() {
		SQLContext sqlContext = spark.sqlContext();
		UDFRegistration.initSedona(sqlContext);
		Dataset<Row> sql = sqlContext.sql("SELECT ST_GeomFromWKT('POINT(0 1)')");
		List<Row> rows = sql.collectAsList();
		System.out.println("------------------------------------------------------------");
		System.out.println(rows);

		sql("CREATE TABLE %s (id bigint, data string, geo geometry) USING iceberg TBLPROPERTIES ('read.parquet.vectorization.enabled' = 'false')", tableName);
		// sql("CREATE TABLE %s (id bigint, data string, geo geometry) USING iceberg PARTITIONED BY (xz2(12, geo)) TBLPROPERTIES ('read.parquet.vectorization.enabled' = 'false')", tableName);

		Dataset<Row> tableDf = spark.table(this.tableName);
		StructType schema = tableDf.schema();

		String baseTable = tableName("base_geo");
		sql("CREATE TABLE %s (id bigint, data string, geo_wkt string) USING iceberg", baseTable);
		int n = 3;
		for (long k = 0; k < n; k++) {
			String geo = String.format("POINT (%d %d)", k, k + 1);
			String insertSql = String.format("INSERT INTO %s values (%d, '%s', '%s')", baseTable, k, "str_" + k, geo);
			System.out.println("------------------------------------------------------------");
			System.out.println(insertSql);
			sqlContext.sql(insertSql);
		}
		Dataset<Row> baseTableDf = spark.table(baseTable);
		baseTableDf.show();

		String insert = String.format("INSERT INTO %s SELECT id, data, ST_GeomFromWKT(geo_wkt) FROM %s", this.tableName, baseTable);
		System.out.println("------------------------------------------------------------");
		System.out.println(insert);
		sqlContext.sql(insert);
		tableDf.show();
	}

}
