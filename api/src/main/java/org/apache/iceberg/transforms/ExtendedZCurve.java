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
package org.apache.iceberg.transforms;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.transforms.geometry.IndexRangeSet;
import org.apache.iceberg.transforms.geometry.XZ2SFCurving;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

// xz2-ordering index
class ExtendedZCurve<T> implements Transform<T, Long> {
  private final int resolution;
  private final XZ2SFCurving xz2sfc;

  ExtendedZCurve(int resolution) {
    this.resolution = resolution;
    this.xz2sfc = new XZ2SFCurving(resolution);
  }

  static ExtendedZCurve get(Type type, int resolution) {
    Preconditions.checkArgument(
        resolution > 0, "Invalid truncate width: %s (must be > 0)", resolution);
    if (type.typeId() == Type.TypeID.GEOMETRY) {
      return new ExtendedZCurve(resolution);
    } else {
      throw new UnsupportedOperationException("Cannot implement xz partition on type: " + type);
    }
  }

  public int getResolution() {
    return resolution;
  }

  @Override
  public Long apply(T value) {
    Geometry geom = null;
    if (value instanceof Geometry) {
      geom = (Geometry) value;
    } else if (value instanceof ByteBuffer) {
      geom = TypeUtil.GeometryUtils.byteBuffer2geometry((ByteBuffer) value);
    }
    if (geom == null) {
      return 0L;
    }
    Envelope envelope = geom.getEnvelopeInternal();
    return xz2sfc.index(
        envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.GEOMETRY;
  }

  @Override
  public boolean preservesOrder() {
    return false;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.LongType.get();
  }

  @Override
  public UnboundRangePredicate project(String name, BoundPredicate<T> predicate) {
    Expression.Operation op = predicate.op();
    List<IndexRangeSet.Interval> intervals = getRange(predicate);
    if (intervals == null) {
      return null;
    }
    switch (op) {
      case ST_WITHIN:
      case ST_INTERSECTS:
        return new UnboundRangePredicate(
            predicate.op(), Expressions.ref(name), new IndexRangeSet(intervals));
      case ST_CONTAINS:
        IndexRangeSet rangeSet =
            new IndexRangeSet(
                intervals.stream()
                    .filter(r -> !r.getLevel().equals(IndexRangeSet.IntervalLevel.WITHIN))
                    .collect(Collectors.toList()));
        return new UnboundRangePredicate(predicate.op(), Expressions.ref(name), rangeSet);
    }
    return null;
  }

  @Override
  public UnboundRangePredicate projectStrict(String name, BoundPredicate<T> predicate) {
    Expression.Operation op = predicate.op();
    List<IndexRangeSet.Interval> intervals = getRange(predicate);
    if (intervals == null) {
      return null;
    }
    switch (op) {
      case ST_WITHIN:
      case ST_INTERSECTS:
        IndexRangeSet rangeSet =
            new IndexRangeSet(
                intervals.stream()
                    .filter(r -> r.getLevel().equals(IndexRangeSet.IntervalLevel.WITHIN))
                    .collect(Collectors.toList()));
        return new UnboundRangePredicate(predicate.op(), Expressions.ref(name), rangeSet);
      case ST_CONTAINS:
        return null;
    }
    return null;
  }

  private List<IndexRangeSet.Interval> getRange(BoundPredicate<T> predicate) {
    Geometry geom = null;
    if (predicate.isLiteralPredicate()) {
      T value = predicate.asLiteralPredicate().literal().value();
      if (value instanceof ByteBuffer) {
        geom = TypeUtil.GeometryUtils.byteBuffer2geometry((ByteBuffer) value);
      } else if (value instanceof Geometry) {
        geom = (Geometry) value;
      }
      if (geom == null) {
        return null;
      }
      return xz2sfc.ranges(geom);
    }
    return null;
  }

  @Override
  public String toString() {
    return "xz2[" + resolution + "]";
  }
}
