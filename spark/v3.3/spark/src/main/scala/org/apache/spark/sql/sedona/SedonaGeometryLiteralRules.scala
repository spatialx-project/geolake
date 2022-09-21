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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.udt.GeometryUDT
import scala.util.Try

// Catalyst optimization rule for folding constant geometry expressions, such as
// ST_PointFromText('40.7128,-74.0060', ',') or st_makeBBOX(116.3, 39.90, 116.5, 40.1)
object SedonaGeometryLiteralRules extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case q: LogicalPlan => q.transformExpressionsDown {
        case expr: Expression if isSedonaExpression(expr) => tryConstantFolding(expr)
      }
    }
  }

  private def isSedonaExpression(expression: Expression): Boolean =
    expression.getClass.getCanonicalName.startsWith("org.apache.spark.sql.sedona_sql.expressions")

  private def tryConstantFolding(expr: Expression): Expression = Try {
    expr.eval(null) match {
      case data: ArrayData =>
        val ret = GeometryUDT.deserialize(data)
        GeometryLiteral(data, ret)
      case other: Any => Literal(other)
    }
  }.getOrElse(expr)
}
