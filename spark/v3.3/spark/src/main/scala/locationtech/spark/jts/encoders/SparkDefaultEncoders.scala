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

package locationtech.spark.jts.encoders

import java.lang
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import scala.reflect.runtime.universe._

/**
 * These encoders exist only for simplifying the construction of DataFrame/Dataset DSL
 * functions. End users should get their default encoders via the Spark recommended
 * pattern:
 * {{{
 *   val spark: SparkSession = ...
 *   import spark.implicits._
 * }}}
 *
 */
private[jts] trait SparkDefaultEncoders {
  implicit def stringEncoder: Encoder[String] = Encoders.STRING
  implicit def jFloatEncoder: Encoder[lang.Float] = Encoders.FLOAT
  implicit def doubleEncoder: Encoder[Double] = Encoders.scalaDouble
  implicit def jDoubleEncoder: Encoder[lang.Double] = Encoders.DOUBLE
  implicit def intEncoder: Encoder[Int] = Encoders.scalaInt
  implicit def jBooleanEncoder: Encoder[lang.Boolean] = Encoders.BOOLEAN
  implicit def booleanEncoder: Encoder[Boolean] = Encoders.scalaBoolean
  implicit def arrayEncoder[T: TypeTag]: Encoder[Array[T]] = ExpressionEncoder()
}
private[jts] object SparkDefaultEncoders extends SparkDefaultEncoders
