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
package org.apache.iceberg.spark.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.aggregate.Max;
import org.apache.spark.sql.catalyst.expressions.aggregate.Min;
import org.apache.spark.sql.connector.catalog.functions.AggregateFunction;
import org.apache.spark.sql.iceberg.udt.GeometrySerializer;
import org.apache.spark.sql.iceberg.udt.GeometryUDT;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

abstract class AbstractGeomMinMaxAggFunction
    implements AggregateFunction<Double, Double>, AggSupportPartialPushDown {
  @Override
  public Double newAggregationState() {
    return null;
  }

  abstract Double dimensionVal(Envelope env);

  @Override
  public Double produceResult(Double state) {
    return state;
  }

  @Override
  public DataType[] inputTypes() {
    return new DataType[] {new GeometryUDT()};
  }

  @Override
  public DataType resultType() {
    return DoubleType$.MODULE$;
  }

  public abstract static class MinGeomFunction extends AbstractGeomMinMaxAggFunction {
    @Override
    public Double update(Double state, InternalRow input) {
      if (input.isNullAt(0)) {
        return state;
      }
      Geometry geom = GeometrySerializer.deserialize(input.getBinary(0));
      if (state == null) {
        return dimensionVal(geom.getEnvelopeInternal());
      }
      return Math.min(dimensionVal(geom.getEnvelopeInternal()), state);
    }

    @Override
    public Double merge(Double leftState, Double rightState) {
      if (leftState == null) {
        return rightState;
      } else if (rightState == null) {
        return leftState;
      }
      return Math.min(leftState, rightState);
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
        aggPartialAttribute(AttributeReference attr) {
      return new Min(attr);
    }
  }

  public abstract static class MaxGeomFunction extends AbstractGeomMinMaxAggFunction {
    @Override
    public Double update(Double state, InternalRow input) {
      if (input.isNullAt(0)) {
        return state;
      }
      Geometry geom = GeometrySerializer.deserialize(input.getBinary(0));
      if (state == null) {
        return dimensionVal(geom.getEnvelopeInternal());
      }
      return Math.max(dimensionVal(geom.getEnvelopeInternal()), state);
    }

    @Override
    public Double merge(Double leftState, Double rightState) {
      if (leftState == null) {
        return rightState;
      } else if (rightState == null) {
        return leftState;
      }
      return Math.max(leftState, rightState);
    }

    @Override
    public org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
        aggPartialAttribute(AttributeReference attr) {
      return new Max(attr);
    }
  }
}
