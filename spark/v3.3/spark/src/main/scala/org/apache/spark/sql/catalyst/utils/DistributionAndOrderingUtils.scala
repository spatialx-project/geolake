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

package org.apache.spark.sql.catalyst.utils

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.IcebergBucketTransform
import org.apache.spark.sql.catalyst.expressions.IcebergDayTransform
import org.apache.spark.sql.catalyst.expressions.IcebergHourTransform
import org.apache.spark.sql.catalyst.expressions.IcebergMonthTransform
import org.apache.spark.sql.catalyst.expressions.IcebergTruncateTransform
import org.apache.spark.sql.catalyst.expressions.IcebergXZ2Transform
import org.apache.spark.sql.catalyst.expressions.IcebergYearTransform
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.connector.distributions.ClusteredDistribution
import org.apache.spark.sql.connector.distributions.Distribution
import org.apache.spark.sql.connector.distributions.OrderedDistribution
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.expressions.ApplyTransform
import org.apache.spark.sql.connector.expressions.BucketTransform
import org.apache.spark.sql.connector.expressions.DaysTransform
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.HoursTransform
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.connector.expressions.Literal
import org.apache.spark.sql.connector.expressions.MonthsTransform
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.NullOrdering
import org.apache.spark.sql.connector.expressions.SortDirection
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.YearsTransform
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import scala.collection.compat.immutable.ArraySeq

object DistributionAndOrderingUtils {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def prepareQuery(
      requiredDistribution: Distribution,
      requiredOrdering: Array[SortOrder],
      query: LogicalPlan,
      conf: SQLConf): LogicalPlan = {

    val resolver = conf.resolver

    val distribution = requiredDistribution match {
      case d: OrderedDistribution =>
        d.ordering.map(e => toCatalyst(e, query, resolver))
      case d: ClusteredDistribution =>
        d.clustering.map(e => toCatalyst(e, query, resolver))
      case _: UnspecifiedDistribution =>
        Array.empty[catalyst.expressions.Expression]
    }

    val queryWithDistribution = if (distribution.nonEmpty) {
      // the conversion to catalyst expressions above produces SortOrder expressions
      // for OrderedDistribution and generic expressions for ClusteredDistribution
      // this allows RepartitionByExpression to pick either range or hash partitioning
      RepartitionByExpression(distribution.toSeq, query, None)
    } else {
      query
    }

    val ordering = requiredOrdering
      .map(e => toCatalyst(e, query, resolver).asInstanceOf[catalyst.expressions.SortOrder])

    val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
      Sort(ArraySeq.unsafeWrapArray(ordering), global = false, queryWithDistribution)
    } else {
      queryWithDistribution
    }

    queryWithDistributionAndOrdering
  }

  private def toCatalyst(
      expr: Expression,
      query: LogicalPlan,
      resolver: Resolver): catalyst.expressions.Expression = {

    // we cannot perform the resolution in the analyzer since we need to optimize expressions
    // in nodes like OverwriteByExpression before constructing a logical write
    def resolve(parts: Seq[String]): NamedExpression = {
      query.resolve(parts, resolver) match {
        case Some(attr) =>
          attr
        case None =>
          val ref = parts.quoted
          throw new AnalysisException(s"Cannot resolve '$ref' using ${query.output}")
      }
    }

    expr match {
      case s: SortOrder =>
        val catalystChild = toCatalyst(s.expression(), query, resolver)
        catalyst.expressions.SortOrder(catalystChild, toCatalyst(s.direction), toCatalyst(s.nullOrdering), Seq.empty)
      case it: IdentityTransform =>
        resolve(ArraySeq.unsafeWrapArray(it.ref.fieldNames))
      case BucketTransform(numBuckets, ref) =>
        IcebergBucketTransform(numBuckets, resolve(ArraySeq.unsafeWrapArray(ref.fieldNames)))
      case TruncateTransform(ref, width) =>
        IcebergTruncateTransform(resolve(ArraySeq.unsafeWrapArray(ref.fieldNames)), width)
      case XZ2Transform(ref, resolution) =>
        IcebergXZ2Transform(resolve(ArraySeq.unsafeWrapArray(ref.fieldNames)), resolution)
      case yt: YearsTransform =>
        IcebergYearTransform(resolve(ArraySeq.unsafeWrapArray(yt.ref.fieldNames)))
      case mt: MonthsTransform =>
        IcebergMonthTransform(resolve(ArraySeq.unsafeWrapArray(mt.ref.fieldNames)))
      case dt: DaysTransform =>
        IcebergDayTransform(resolve(ArraySeq.unsafeWrapArray(dt.ref.fieldNames)))
      case ht: HoursTransform =>
        IcebergHourTransform(resolve(ArraySeq.unsafeWrapArray(ht.ref.fieldNames)))
      case ref: FieldReference =>
        resolve(ArraySeq.unsafeWrapArray(ref.fieldNames))
      case _ =>
        throw new RuntimeException(s"$expr is not currently supported")

    }
  }

  private def toCatalyst(direction: SortDirection): catalyst.expressions.SortDirection = {
    direction match {
      case SortDirection.ASCENDING => catalyst.expressions.Ascending
      case SortDirection.DESCENDING => catalyst.expressions.Descending
    }
  }

  private def toCatalyst(nullOrdering: NullOrdering): catalyst.expressions.NullOrdering = {
    nullOrdering match {
      case NullOrdering.NULLS_FIRST => catalyst.expressions.NullsFirst
      case NullOrdering.NULLS_LAST => catalyst.expressions.NullsLast
    }
  }

  private object BucketTransform {
    def unapply(transform: Transform): Option[(Int, FieldReference)] = transform match {
      case bt: BucketTransform => bt.columns match {
        case Seq(nf: NamedReference) =>
          Some(bt.numBuckets.value(), FieldReference(ArraySeq.unsafeWrapArray(nf.fieldNames())))
        case _ =>
          None
      }
      case _ => None
    }
  }

  private object Lit {
    def unapply[T](literal: Literal[T]): Some[(T, DataType)] = {
      Some((literal.value, literal.dataType))
    }
  }

  private class TransformWithIntegerArgument(transformName: String) {
    def unapply(transform: Transform): Option[(FieldReference, Int)] = transform match {
      case at @ ApplyTransform(name, _) if name.equalsIgnoreCase(transformName)  => at.args match {
        case Seq(nf: NamedReference, Lit(value: Int, IntegerType)) =>
          Some(FieldReference(ArraySeq.unsafeWrapArray(nf.fieldNames())), value)
        case Seq(Lit(value: Int, IntegerType), nf: NamedReference) =>
          Some(FieldReference(ArraySeq.unsafeWrapArray(nf.fieldNames())), value)
        case _ =>
          None
      }
      case _ => None
    }
  }

  private object TruncateTransform extends TransformWithIntegerArgument("truncate")

  private object XZ2Transform extends TransformWithIntegerArgument("xz2")
}
