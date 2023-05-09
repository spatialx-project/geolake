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

import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class BoundAggregate<T, C> extends Aggregate<BoundTerm<T>> implements Bound<C> {

  protected BoundAggregate(Operation op, BoundTerm<T> term) {
    super(op, term);
  }

  @Override
  public C eval(StructLike struct) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement eval(StructLike)");
  }

  C eval(DataFile file) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement eval(DataFile)");
  }

  boolean hasValue(DataFile file) {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement hasValue(DataFile)");
  }

  Aggregator<C> newAggregator() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement newAggregator()");
  }

  @Override
  public BoundReference<?> ref() {
    return term().ref();
  }

  public Type type() {
    switch (op()) {
      case COUNT:
      case COUNT_STAR:
        return Types.LongType.get();
      case ST_MINX:
      case ST_MINY:
      case ST_MAXX:
      case ST_MAXY:
        return Types.DoubleType.get();
      default:
        return resultType(term().type());
    }
  }

  private Type resultType(Type columnType) {
    if (columnType == Types.GeometryType.get()) {
      return Types.GeometryBoundType.get();
    }
    return columnType;
  }

  public String columnName() {
    if (op() == Operation.COUNT_STAR) {
      return "*";
    } else {
      return ref().name();
    }
  }

  public String describe() {
    switch (op()) {
      case COUNT_STAR:
        return "count(*)";
      case COUNT:
        return "count(" + ExpressionUtil.describe(term()) + ")";
      case MAX:
        return "max(" + ExpressionUtil.describe(term()) + ")";
      case MIN:
        return "min(" + ExpressionUtil.describe(term()) + ")";
      case ST_MINX:
        return "st_minx(" + ExpressionUtil.describe(term()) + ")";
      case ST_MAXX:
        return "st_maxx(" + ExpressionUtil.describe(term()) + ")";
      case ST_MINY:
        return "st_miny(" + ExpressionUtil.describe(term()) + ")";
      case ST_MAXY:
        return "st_maxy(" + ExpressionUtil.describe(term()) + ")";
      default:
        throw new UnsupportedOperationException("Unsupported aggregate type: " + op());
    }
  }

  <V> V safeGet(Map<Integer, V> map, int key) {
    return safeGet(map, key, null);
  }

  <V> V safeGet(Map<Integer, V> map, int key, V defaultValue) {
    if (map != null) {
      return map.getOrDefault(key, defaultValue);
    }

    return null;
  }

  interface Aggregator<R> {
    void update(StructLike struct);

    void update(DataFile file);

    boolean hasValue(DataFile file);

    R result();

    boolean isValid();
  }

  abstract static class NullSafeAggregator<T, R> implements Aggregator<R> {
    private final BoundAggregate<T, R> aggregate;
    private boolean isValid = true;

    NullSafeAggregator(BoundAggregate<T, R> aggregate) {
      this.aggregate = aggregate;
    }

    protected abstract void update(R value);

    protected abstract R current();

    @Override
    public void update(StructLike struct) {
      R value = aggregate.eval(struct);
      if (value != null) {
        update(value);
      }
    }

    @Override
    public boolean hasValue(DataFile file) {
      return aggregate.hasValue(file);
    }

    @Override
    public void update(DataFile file) {
      if (isValid) {
        if (hasValue(file)) {
          R value = aggregate.eval(file);
          if (value != null) {
            update(value);
          }
        } else {
          this.isValid = false;
        }
      }
    }

    @Override
    public R result() {
      if (!isValid) {
        return null;
      }

      return current();
    }

    @Override
    public boolean isValid() {
      return this.isValid;
    }
  }
}
