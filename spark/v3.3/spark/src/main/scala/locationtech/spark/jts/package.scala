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

package locationtech.spark

import locationtech.spark.jts.encoders.SpatialEncoders
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


/**
 * User-facing module imports, sufficient for accessing the standard Spark-JTS functionality.
 */
package object jts extends DataFrameFunctions.Library with SpatialEncoders {
  /**
   * Initialization function that must be called before any JTS functionality
   * is accessed. This function can be called directly, or one of the `initJTS`
   * enrichment methods on [[SQLContext]] or [[SparkSession]] can be used instead.
   */
  def initJTS(sqlContext: SQLContext): Unit = {
    org.apache.spark.sql.jts.registerTypes()
//    udf.registerFunctions(sqlContext)
    rules.registerOptimizations(sqlContext)
  }

  def initJTS(spark: SparkSession): SparkSession = {
    org.apache.spark.sql.jts.registerTypes()
    rules.registerOptimizations(spark.sqlContext)
    spark
  }

  /** Enrichment over [[SQLContext]] to add `withJTS` "literate" method. */
  implicit class SQLContextWithJTS(val sqlContext: SQLContext) extends AnyVal {
    def withJTS: SQLContext = {
      initJTS(sqlContext)
      sqlContext
    }
  }

  /** Enrichment over [[SparkSession]] to add `withJTS` "literate" method. */
  implicit class SparkSessionWithJTS(val spark: SparkSession) extends AnyVal {
    def withJTS: SparkSession = {
      initJTS(spark.sqlContext)
      spark
    }
  }
}

