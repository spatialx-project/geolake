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

import org.apache.iceberg.spark.SparkAggregates
import org.apache.iceberg.spark.functions.AggSupportPartialPushDown
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.aggregate
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.V2Aggregator
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.expressions.aggregate.Avg
import org.apache.spark.sql.connector.expressions.aggregate.Count
import org.apache.spark.sql.connector.expressions.aggregate.CountStar
import org.apache.spark.sql.connector.expressions.aggregate.Max
import org.apache.spark.sql.connector.expressions.aggregate.Min
import org.apache.spark.sql.connector.expressions.aggregate.Sum
import org.apache.spark.sql.connector.expressions.aggregate.UserDefinedAggregateFunc
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.ScanBuilderHolder
import org.apache.spark.sql.execution.datasources.v2.V2ScanRelationPushDown
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DecimalType
import scala.collection.mutable

/**
 * Push down spatial aggregates such as
 * [[org.apache.iceberg.spark.functions.GeomMinMax.GeomMaxXFunction GeomMaxXFunction]],
 * [[org.apache.iceberg.spark.functions.GeomMinMax.GeomMinXFunction GeomMinXFunction]],
 * [[org.apache.iceberg.spark.functions.GeomMinMax.GeomMaxYFunction GeomMaxYFunction]],
 * [[org.apache.iceberg.spark.functions.GeomMinMax.GeomMinYFunction GeomMinYFunction]]
 * to iceberg.
 */
object GeometryAggregatePushDown extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val pushdownRules = Seq[LogicalPlan => LogicalPlan](
      createScanBuilder,
      pushDownGeometryAggregates,
      V2ScanRelationPushDown.pushDownLimitAndOffset,
      V2ScanRelationPushDown.buildScanWithPushedAggregate,
      V2ScanRelationPushDown.pruneColumns
    )

    pushdownRules.foldLeft(plan) { (newPlan, pushDownRule) =>
      pushDownRule(newPlan)
    }
  }

  private def createScanBuilder(plan: LogicalPlan) = plan.transform {
    case DataSourceV2ScanRelation(r, _: org.apache.iceberg.spark.source.SparkBatchQueryScan, _, _, _) =>
      ScanBuilderHolder(r.output, r, r.table.asReadable.newScanBuilder(r.options))
  }

  def pushDownGeometryAggregates(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // update the scan builder with agg pushdown and return a new plan with agg pushed
      case agg: Aggregate => rewriteAggregate(agg)
    }
  }

  def rewriteAggregate(agg: Aggregate): LogicalPlan = agg.child match {
    case PhysicalOperation(project, Nil, holder@ScanBuilderHolder(_, _,
    r: SupportsPushDownAggregates)) if CollapseProject.canCollapseExpressions(
      agg.aggregateExpressions, project, alwaysInline = true) =>
      val aliasMap = getAliasMap(project)
      val actualResultExprs = agg.aggregateExpressions.map(replaceAliasButKeepName(_, aliasMap))
      val actualGroupExprs = agg.groupingExpressions.map(replaceAlias(_, aliasMap))

      val aggExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      val aggregates = collectAggregates(actualResultExprs, aggExprToOutputOrdinal)
      val normalizedAggExprs = DataSourceStrategy.normalizeExprs(
        aggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
      val normalizedGroupingExpr = DataSourceStrategy.normalizeExprs(
        actualGroupExprs, holder.relation.output)
      val translatedAggOpt = DataSourceStrategy.translateAggregation(
        normalizedAggExprs, normalizedGroupingExpr)
      if (translatedAggOpt.isEmpty) {
        // Cannot translate the catalyst aggregate, return the query plan unchanged.
        return agg
      }

      val (finalResultExprs, finalAggExprs, translatedAgg, canCompletePushDown) = {
        if (r.supportCompletePushDown(translatedAggOpt.get)) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, true)
        } else if (!translatedAggOpt.get.aggregateExpressions().exists(_.isInstanceOf[Avg])) {
          (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
        } else {
          // scalastyle:off
          // The data source doesn't support the complete push-down of this aggregation.
          // Here we translate `AVG` to `SUM / COUNT`, so that it's more likely to be
          // pushed, completely or partially.
          // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
          // SELECT avg(c1) FROM t GROUP BY c2;
          // The original logical plan is
          // Aggregate [c2#10],[avg(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          //
          // After convert avg(c1#9) to sum(c1#9)/count(c1#9)
          // we have the following
          // Aggregate [c2#10],[sum(c1#9)/count(c1#9) AS avg(c1)#19]
          // +- ScanOperation[...]
          // scalastyle:on
          val newResultExpressions = actualResultExprs.map { expr =>
            expr.transform {
              case AggregateExpression(avg: aggregate.Average, _, isDistinct, _, _) =>
                val sum = aggregate.Sum(avg.child).toAggregateExpression(isDistinct)
                val count = aggregate.Count(avg.child).toAggregateExpression(isDistinct)
                avg.evaluateExpression transform {
                  case a: Attribute if a.semanticEquals(avg.sum) =>
                    addCastIfNeeded(sum, avg.sum.dataType)
                  case a: Attribute if a.semanticEquals(avg.count) =>
                    addCastIfNeeded(count, avg.count.dataType)
                }
            }
          }.asInstanceOf[Seq[NamedExpression]]
          // Because aggregate expressions changed, translate them again.
          aggExprToOutputOrdinal.clear()
          val newAggregates =
            collectAggregates(newResultExpressions, aggExprToOutputOrdinal)
          val newNormalizedAggExprs = DataSourceStrategy.normalizeExprs(
            newAggregates, holder.relation.output).asInstanceOf[Seq[AggregateExpression]]
          val newTranslatedAggOpt = DataSourceStrategy.translateAggregation(
            newNormalizedAggExprs, normalizedGroupingExpr)
          if (newTranslatedAggOpt.isEmpty) {
            // Ideally we should never reach here. But if we end up with not able to translate
            // new aggregate with AVG replaced by SUM/COUNT, revert to the original one.
            (actualResultExprs, normalizedAggExprs, translatedAggOpt.get, false)
          } else {
            (newResultExpressions, newNormalizedAggExprs, newTranslatedAggOpt.get,
              r.supportCompletePushDown(newTranslatedAggOpt.get))
          }
        }
      }

      if (!canCompletePushDown && !supportPartialAggPushDown(translatedAgg)) {
        return agg
      }
      if (!r.pushAggregation(translatedAgg)) {
        return agg
      }

      // scalastyle:off
      // We name the output columns of group expressions and aggregate functions by
      // ordinal: `group_col_0`, `group_col_1`, ..., `agg_func_0`, `agg_func_1`, ...
      // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
      // SELECT min(c1), max(c1) FROM t GROUP BY c2;
      // Use group_col_0, agg_func_0, agg_func_1 as output for ScanBuilderHolder.
      // We want to have the following logical plan:
      // == Optimized Logical Plan ==
      // Aggregate [group_col_0#10], [min(agg_func_0#21) AS min(c1)#17, max(agg_func_1#22) AS max(c1)#18]
      // +- ScanBuilderHolder[group_col_0#10, agg_func_0#21, agg_func_1#22]
      // Later, we build the `Scan` instance and convert ScanBuilderHolder to DataSourceV2ScanRelation.
      // scalastyle:on
      val groupOutputMap = normalizedGroupingExpr.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"group_col_$i", e.dataType)() -> e
      }
      val groupOutput = groupOutputMap.unzip._1
      val aggOutputMap = finalAggExprs.zipWithIndex.map { case (e, i) =>
        AttributeReference(s"agg_func_$i", e.dataType)() -> e
      }
      val aggOutput = aggOutputMap.unzip._1
      val newOutput = groupOutput ++ aggOutput
      val groupByExprToOutputOrdinal = mutable.HashMap.empty[Expression, Int]
      normalizedGroupingExpr.zipWithIndex.foreach { case (expr, ordinal) =>
        if (!groupByExprToOutputOrdinal.contains(expr.canonicalized)) {
          groupByExprToOutputOrdinal(expr.canonicalized) = ordinal
        }
      }

      holder.pushedAggregate = Some(translatedAgg)
      holder.pushedAggOutputMap = AttributeMap(groupOutputMap ++ aggOutputMap)
      holder.output = newOutput
      logInfo(
        s"""
           |Pushing operators to ${holder.relation.name}
           |Pushed Aggregate Functions:
           | ${translatedAgg.aggregateExpressions().mkString(", ")}
           |Pushed Group by:
           | ${translatedAgg.groupByExpressions.mkString(", ")}
         """.stripMargin)

      if (canCompletePushDown) {
        val projectExpressions = finalResultExprs.map { expr =>
          expr.transformDown {
            case agg: AggregateExpression =>
              val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
              Alias(aggOutput(ordinal), agg.resultAttribute.name)(agg.resultAttribute.exprId)
            case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
              val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
              expr match {
                case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
                case _ => groupOutput(ordinal)
              }
          }
        }.asInstanceOf[Seq[NamedExpression]]
        Project(projectExpressions, holder)
      } else {
        // scalastyle:off
        // Change the optimized logical plan to reflect the pushed down aggregate
        // e.g. TABLE t (c1 INT, c2 INT, c3 INT)
        // SELECT min(c1), max(c1) FROM t GROUP BY c2;
        // The original logical plan is
        // Aggregate [c2#10],[min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c1#9, c2#10] ...
        //
        // After change the V2ScanRelation output to [c2#10, min(c1)#21, max(c1)#22]
        // we have the following
        // !Aggregate [c2#10], [min(c1#9) AS min(c1)#17, max(c1#9) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        //
        // We want to change it to
        // == Optimized Logical Plan ==
        // Aggregate [c2#10], [min(min(c1)#21) AS min(c1)#17, max(max(c1)#22) AS max(c1)#18]
        // +- RelationV2[c2#10, min(c1)#21, max(c1)#22] ...
        // scalastyle:on
        val aggExprs = finalResultExprs.map(_.transform {
          case agg: AggregateExpression =>
            val ordinal = aggExprToOutputOrdinal(agg.canonicalized)
            val aggAttribute = aggOutput(ordinal)
            val aggFunction: aggregate.AggregateFunction =
              agg.aggregateFunction match {
                case max: aggregate.Max =>
                  max.copy(child = aggAttribute)
                case min: aggregate.Min =>
                  min.copy(child = aggAttribute)
                case sum: aggregate.Sum =>
                  // To keep the dataType of `Sum` unchanged, we need to cast the
                  // data-source-aggregated result to `Sum.child.dataType` if it's decimal.
                  // See `SumBase.resultType`
                  val newChild = if (sum.dataType.isInstanceOf[DecimalType]) {
                    addCastIfNeeded(aggAttribute, sum.child.dataType)
                  } else {
                    aggAttribute
                  }
                  sum.copy(child = newChild)
                case _: aggregate.Count =>
                  aggregate.Sum(aggAttribute)
                // user defined aggregate function
                case udaf: V2Aggregator[?, ?] =>
                  udaf.aggrFunc match {
                    case down: AggSupportPartialPushDown =>
                      down.aggPartialAttribute(aggAttribute)
                    case _ =>
                      udaf
                  }
                case other => other
              }
            agg.copy(aggregateFunction = aggFunction)
          case expr if groupByExprToOutputOrdinal.contains(expr.canonicalized) =>
            val ordinal = groupByExprToOutputOrdinal(expr.canonicalized)
            expr match {
              case ne: NamedExpression => Alias(groupOutput(ordinal), ne.name)(ne.exprId)
              case _ => groupOutput(ordinal)
            }
        }).asInstanceOf[Seq[NamedExpression]]
        Aggregate(groupOutput, aggExprs, holder)
      }

    case _ => agg
  }

  private def collectAggregates(
                                 resultExpressions: Seq[NamedExpression],
                                 aggExprToOutputOrdinal: mutable.HashMap[Expression, Int]): Seq[AggregateExpression] = {
    var ordinal = 0
    resultExpressions.flatMap { expr =>
      expr.collect {
        // Do not push down duplicated aggregate expressions. For example,
        // `SELECT max(a) + 1, max(a) + 2 FROM ...`, we should only push down one
        // `max(a)` to the data source.
        case agg: AggregateExpression
          if !aggExprToOutputOrdinal.contains(agg.canonicalized) =>
          aggExprToOutputOrdinal(agg.canonicalized) = ordinal
          ordinal += 1
          agg
      }
    }
  }

  private def addCastIfNeeded(expression: Expression, expectedDataType: DataType) =
    if (expression.dataType == expectedDataType) {
      expression
    } else {
      Cast(expression, expectedDataType)
    }

  private def supportPartialAggPushDown(agg: Aggregation): Boolean = {
    // We can only partially push down min/max/sum/count without DISTINCT.
    agg.aggregateExpressions().isEmpty || agg.aggregateExpressions().forall {
      case sum: Sum => !sum.isDistinct
      case count: Count => !count.isDistinct
      case _: Min | _: Max | _: CountStar => true
      case udaf: UserDefinedAggregateFunc => isUdafSupportPartialPushDown(udaf)
      case _ => false
    }
  }

  private def isUdafSupportPartialPushDown(udaf: UserDefinedAggregateFunc): Boolean = {
    // It is better to get the AggregateFunction and check if it implement the `AggSupportPartialPushDown` interface.
    // However, we can not get the AggregateFunction only if we add an attribute in `UserDefinedAggregateFunc`
    SparkAggregates.USER_DEFINED_AGGREGATES.containsKey(udaf.name())
  }
}
