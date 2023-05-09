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
package org.apache.iceberg.spark;

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.functions.GeomMinMax;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Count;
import org.apache.spark.sql.connector.expressions.aggregate.CountStar;
import org.apache.spark.sql.connector.expressions.aggregate.Max;
import org.apache.spark.sql.connector.expressions.aggregate.Min;
import org.apache.spark.sql.connector.expressions.aggregate.UserDefinedAggregateFunc;

public class SparkAggregates {
  private SparkAggregates() {}

  private static final Map<Class<? extends AggregateFunc>, Operation> AGGREGATES =
      ImmutableMap.<Class<? extends AggregateFunc>, Operation>builder()
          .put(Count.class, Operation.COUNT)
          .put(CountStar.class, Operation.COUNT_STAR)
          .put(Max.class, Operation.MAX)
          .put(Min.class, Operation.MIN)
          .buildOrThrow();
  public static final Map<String, Operation> USER_DEFINED_AGGREGATES =
      ImmutableMap.<String, Operation>builder()
          .put(GeomMinMax.MinX, Operation.ST_MINX)
          .put(GeomMinMax.MinY, Operation.ST_MINY)
          .put(GeomMinMax.MaxX, Operation.ST_MAXX)
          .put(GeomMinMax.MaxY, Operation.ST_MAXY)
          .buildOrThrow();

  public static Expression convert(AggregateFunc aggregate) {
    Operation op =
        aggregate instanceof UserDefinedAggregateFunc
            ? USER_DEFINED_AGGREGATES.get(((UserDefinedAggregateFunc) aggregate).name())
            : AGGREGATES.get(aggregate.getClass());
    if (op != null) {
      switch (op) {
        case COUNT:
          Count countAgg = (Count) aggregate;
          if (countAgg.isDistinct()) {
            // manifest file doesn't have count distinct so this can't be pushed down
            return null;
          }

          if (countAgg.column() instanceof NamedReference) {
            return Expressions.count(SparkUtil.toColumnName((NamedReference) countAgg.column()));
          } else {
            return null;
          }

        case COUNT_STAR:
          return Expressions.countStar();

        case MAX:
          Max maxAgg = (Max) aggregate;
          if (maxAgg.column() instanceof NamedReference) {
            return Expressions.max(SparkUtil.toColumnName((NamedReference) maxAgg.column()));
          } else {
            return null;
          }

        case MIN:
          Min minAgg = (Min) aggregate;
          if (minAgg.column() instanceof NamedReference) {
            return Expressions.min(SparkUtil.toColumnName((NamedReference) minAgg.column()));
          } else {
            return null;
          }
        case ST_MINX:
        case ST_MAXX:
        case ST_MINY:
        case ST_MAXY:
          UserDefinedAggregateFunc udaf = (UserDefinedAggregateFunc) aggregate;
          NamedReference[] references = udaf.references();
          if (references.length == 1) {
            String columnName = SparkUtil.toColumnName(references[0]);
            switch (op) {
              case ST_MINX:
                return Expressions.stMinX(columnName);
              case ST_MAXX:
                return Expressions.stMaxX(columnName);
              case ST_MINY:
                return Expressions.stMinY(columnName);
              case ST_MAXY:
                return Expressions.stMaxY(columnName);
              default:
                return null;
            }
          } else {
            return null;
          }
      }
    }

    return null;
  }
}
