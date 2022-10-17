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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.iceberg.expressions.{Expression => IcebergExpression}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.source.SparkBatchQueryScan
import org.apache.iceberg.spark.source.SparkScan
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IcebergSTCoveredBy
import org.apache.spark.sql.catalyst.expressions.IcebergSTCovers
import org.apache.spark.sql.catalyst.expressions.IcebergSTIntersects
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.iceberg.udt.GeometrySerializer

/**
 * Push down spatial predicates such as
 * [[org.apache.spark.sql.catalyst.expressions.IcebergSTCovers IcebergSTCovers]],
 * [[org.apache.spark.sql.catalyst.expressions.IcebergSTIntersects IcebergSTIntersects]],
 * [[org.apache.spark.sql.catalyst.expressions.IcebergSTCoveredBy IcebergSTCoveredBy]]
 * to iceberg relation scan node [[SparkBatchQueryScan]].
 */
object GeometryPredicatePushDown extends Rule[LogicalPlan] with PredicateHelper {

  import scala.collection.JavaConverters._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, scanRel: DataSourceV2ScanRelation) if isIcebergRelation(scanRel.relation) =>
      val scan = scanRel.scan.asInstanceOf[SparkScan]
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, scanRel.output)
      val (_, normalizedFiltersWithoutSubquery) = normalizedFilters.partition(SubqueryExpression.hasSubquery)
      val icebergSpatialPredicates = filtersToIcebergSpatialPredicates(
        normalizedFiltersWithoutSubquery, nestedPredicatePushdownEnabled = true)
      val newScan = scan.withExpressions(icebergSpatialPredicates.asJava)
      if (newScan != scan) {
        Filter(condition, scanRel.copy(scan = newScan))
      } else {
        filter
      }
  }

  def isIcebergRelation(plan: LogicalPlan): Boolean = {
    def isIcebergTable(relation: DataSourceV2Relation): Boolean = relation.table match {
      case t: RowLevelOperationTable => t.table.isInstanceOf[SparkTable]
      case _: SparkTable => true
      case _ => false
    }

    plan match {
      case s: SubqueryAlias => isIcebergRelation(s.child)
      case r: DataSourceV2Relation => isIcebergTable(r)
      case _ => false
    }
  }

  def filtersToIcebergSpatialPredicates(
    predicates: Seq[Expression],
    nestedPredicatePushdownEnabled: Boolean): Seq[IcebergExpression] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled)
    predicates.flatMap { predicate => translateToIcebergSpatialPredicate(predicate, pushableColumn) }
  }

  def translateToIcebergSpatialPredicate(
    predicate: Expression,
    pushableColumn: PushableColumnBase): Option[IcebergExpression] = {
    predicate match {
      case And(left, right) =>
        for {
          icebergLeft <- translateToIcebergSpatialPredicate(left, pushableColumn)
          icebergRight <- translateToIcebergSpatialPredicate(right, pushableColumn)
        } yield Expressions.and(icebergLeft, icebergRight)

      case Or(left, right) =>
        for {
          icebergLeft <- translateToIcebergSpatialPredicate(left, pushableColumn)
          icebergRight <- translateToIcebergSpatialPredicate(right, pushableColumn)
        } yield Expressions.or(icebergLeft, icebergRight)

      case Not(innerPredicate) =>
        translateToIcebergSpatialPredicate(innerPredicate, pushableColumn).map(Expressions.not)

      case IcebergSTCovers(pushableColumn(name), Literal(v, _)) =>
        Some(Expressions.stCovers(unquote(name), GeometrySerializer.deserialize(v)))

      case IcebergSTCovers(Literal(v, _), pushableColumn(name)) =>
        Some(Expressions.stCoveredBy(unquote(name), GeometrySerializer.deserialize(v)))

      case IcebergSTCoveredBy(pushableColumn(name), Literal(v, _)) =>
        Some(Expressions.stCoveredBy(unquote(name), GeometrySerializer.deserialize(v)))

      case IcebergSTCoveredBy(Literal(v, _), pushableColumn(name)) =>
        Some(Expressions.stCovers(unquote(name), GeometrySerializer.deserialize(v)))

      case IcebergSTIntersects(pushableColumn(name), Literal(v, _)) =>
        Some(Expressions.stIntersects(unquote(name), GeometrySerializer.deserialize(v)))

      case IcebergSTIntersects(Literal(v, _), pushableColumn(name)) =>
        Some(Expressions.stIntersects(unquote(name), GeometrySerializer.deserialize(v)))

      case _ => None
    }
  }

  private def unquote(name: String): String = {
    parseColumnPath(name).mkString(".")
  }
}
