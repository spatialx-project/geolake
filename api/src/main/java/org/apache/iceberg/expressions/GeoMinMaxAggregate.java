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
package org.apache.iceberg.expressions;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types;

import java.util.Comparator;

public class GeoMinMaxAggregate<T> extends ValueAggregate<T> {
  private final int fieldId;

  private final PrimitiveType originalType;

  private final PrimitiveType type = Types.DoubleType.get();
  private final Comparator<T> comparator;

  private final Operation geomOp;

  protected GeoMinMaxAggregate(BoundTerm<T> term, Operation op) {
    super(op, term);
    Types.NestedField field = term.ref().field();
    this.fieldId = field.fieldId();
    this.originalType = field.type().asPrimitiveType();
    this.geomOp = op;
    this.comparator = Comparators.forType(type);
  }

  @Override
  protected boolean hasValue(DataFile file) {
    return hasValue(file, fieldId);
  }

  @Override
  protected Object evaluateRef(DataFile file) {
    if (!originalType.equals(Types.GeometryType.get())) {
      throw new UnsupportedOperationException("MinGeoXAggregate only supports geometry types");
    }
    Pair<Double, Double> res;
    if (this.geomOp == Operation.ST_MINX || this.geomOp == Operation.ST_MINY) {
      res = Conversions.fromByteBuffer(Types.GeometryBoundType.get(), safeGet(file.lowerBounds(), fieldId));
      if (res == null) {
        return null;
      }
      return this.geomOp == Operation.ST_MINX ? res.first() : res.second();
    } else if (this.geomOp == Operation.ST_MAXX || this.geomOp == Operation.ST_MAXY) {
      res = Conversions.fromByteBuffer(Types.GeometryBoundType.get(), safeGet(file.upperBounds(), fieldId));
      if (res == null) {
        return null;
      }
      return this.geomOp == Operation.ST_MAXX ? res.first() : res.second();
    } else {
      throw new UnsupportedOperationException("MinGeoXAggregate only supports geometry types");
    }
  }

  @Override
  public Aggregator<T> newAggregator() {
    if (this.geomOp == Operation.ST_MINX || this.geomOp == Operation.ST_MINY) {
      return new MinAggregator<>(this, comparator);
    } else if (this.geomOp == Operation.ST_MAXX || this.geomOp == Operation.ST_MAXY) {
      return new MaxAggregator<>(this, comparator);
    }
    throw new UnsupportedOperationException("MinGeoXAggregate only supports geometry types");
  }
}
