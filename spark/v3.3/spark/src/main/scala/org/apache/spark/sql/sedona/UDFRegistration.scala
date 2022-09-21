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

package org.apache.spark.sql.sedona

import org.apache.sedona.sql.UDF.Catalog
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.udf.GeometricConstructorFunctions

object UDFRegistration {
  // User can specify a common prefix of UDFs/UDAFs introduced by Apache Sedona. For example, when prefix is specified
  // as "Sedona_", ST_Contains function from Apache Sedona will be named as "Sedona_ST_Contains". When prefix is
  // explicitly set to empty, Apache Sedona functions will replace corresponding Spark JTS functions.
  def sedonaUdfPrefix(sqlContext: SQLContext): String =
    sqlContext.getConf("spark.iceberg.sedona.udf.prefix", "sedona_")

  /**
   * Register Geometry UDTs, UDFs, UDAFs and optimization rules for Apache Sedona.
   * @param sqlContext Spark [[SQLContext]] object
   */
  def initSedona(sqlContext: SQLContext): Unit = {
    val prefix = sedonaUdfPrefix(sqlContext)
    registerOptimizations(sqlContext)
    registerUdfs(sqlContext, prefix)
  }

  private def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(SedonaGeometryLiteralRules).foreach { r =>
      if (!sqlContext.experimental.extraOptimizations.contains(r)) {
        sqlContext.experimental.extraOptimizations ++= Seq(r)
      }
    }
    Seq(new JoinQueryDetector(sqlContext.sparkSession)).foreach { s =>
      if(!sqlContext.experimental.extraStrategies.contains(s)) {
        sqlContext.experimental.extraStrategies ++= Seq(s)
      }
    }
  }

  private def registerUdfs(sqlContext: SQLContext, prefix: String): Unit = {
    val sparkSession = sqlContext.sparkSession
    Catalog.expressions.foreach(f => {
      val functionIdentifier = FunctionIdentifier(prefix + f.getClass.getSimpleName.dropRight(1))
      val expressionInfo = new ExpressionInfo(
        f.getClass.getCanonicalName,
        functionIdentifier.database.orNull,
        functionIdentifier.funcName)
      sparkSession.sessionState.functionRegistry.registerFunction(functionIdentifier, expressionInfo, f)
    })
    Catalog.aggregateExpressions_UDAF.foreach(f => sparkSession.udf.register(prefix + f.getClass.getSimpleName, f))
    sqlContext.udf.register("ST_GeomFromWKT", GeometricConstructorFunctions.ST_GeomFromWKT)
  }
}
