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

package locationtech.spark.jts.rules

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.jts.GeometryUDT
import scala.util.Try

object GeometryLiteralRules {

  object ScalaUDFRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan.transform {
        case q: LogicalPlan => q.transformExpressionsDown {
          case s: ScalaUDF =>
            // TODO: Break down by GeometryType
            Try {
              s.eval(null) match {
                // Prior to Spark 3.1.1 GenericInteralRows have been returned
                // Spark 3.1.1 started returning UnsafeRows instead of GenericInteralRows
                case row: InternalRow =>
                  val ret = GeometryUDT.deserialize(row)
                  GeometryLiteral(row, ret)
                case other: Any =>
                  Literal(other)
              }
            }.getOrElse(s)
        }
      }
    }
  }

  private[jts] def registerOptimizations(sqlContext: SQLContext): Unit = {
    Seq(ScalaUDFRule).foreach { r =>
      if(!sqlContext.experimental.extraOptimizations.contains(r)) {
        sqlContext.experimental.extraOptimizations ++= Seq(r)
      }
    }
  }
}
