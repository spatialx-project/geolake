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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundVisitor;
import org.apache.iceberg.transforms.geometry.IndexRangeSet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.NaNUtil;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates an {@link Expression} for data described by a {@link StructType}.
 *
 * <p>Data rows must implement {@link StructLike} and are passed to {@link #eval(StructLike)}.
 *
 * <p>This class is thread-safe.
 */
public class Evaluator implements Serializable {
  private final Expression expr;
  private static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

  public Evaluator(StructType struct, Expression unbound) {
    this.expr = Binder.bind(struct, unbound, true);
  }

  public Evaluator(StructType struct, Expression unbound, boolean caseSensitive) {
    this.expr = Binder.bind(struct, unbound, caseSensitive);
  }

  public boolean eval(StructLike data) {
    return new EvalVisitor().eval(data);
  }

  private class EvalVisitor extends BoundVisitor<Boolean> {
    private StructLike struct;

    private boolean eval(StructLike row) {
      this.struct = row;
      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return true;
    }

    @Override
    public Boolean alwaysFalse() {
      return false;
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(Bound<T> valueExpr) {
      return valueExpr.eval(struct) == null;
    }

    @Override
    public <T> Boolean notNull(Bound<T> valueExpr) {
      return valueExpr.eval(struct) != null;
    }

    @Override
    public <T> Boolean isNaN(Bound<T> valueExpr) {
      return NaNUtil.isNaN(valueExpr.eval(struct));
    }

    @Override
    public <T> Boolean notNaN(Bound<T> valueExpr) {
      return !NaNUtil.isNaN(valueExpr.eval(struct));
    }

    @Override
    public <T> Boolean lt(Bound<T> valueExpr, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(valueExpr.eval(struct), lit.value()) < 0;
    }

    @Override
    public <T> Boolean ltEq(Bound<T> valueExpr, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(valueExpr.eval(struct), lit.value()) <= 0;
    }

    @Override
    public <T> Boolean gt(Bound<T> valueExpr, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(valueExpr.eval(struct), lit.value()) > 0;
    }

    @Override
    public <T> Boolean gtEq(Bound<T> valueExpr, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(valueExpr.eval(struct), lit.value()) >= 0;
    }

    @Override
    public <T> Boolean eq(Bound<T> valueExpr, Literal<T> lit) {
      Comparator<T> cmp = lit.comparator();
      return cmp.compare(valueExpr.eval(struct), lit.value()) == 0;
    }

    @Override
    public <T> Boolean notEq(Bound<T> valueExpr, Literal<T> lit) {
      return !eq(valueExpr, lit);
    }

    @Override
    public <T> Boolean in(Bound<T> valueExpr, Set<T> literalSet) {
      return literalSet.contains(valueExpr.eval(struct));
    }

    @Override
    public <T> Boolean notIn(Bound<T> valueExpr, Set<T> literalSet) {
      return !in(valueExpr, literalSet);
    }

    @Override
    public <T> Boolean startsWith(Bound<T> valueExpr, Literal<T> lit) {
      T evalRes = valueExpr.eval(struct);
      return evalRes != null && ((String) evalRes).startsWith((String) lit.value());
    }

    @Override
    public <T> Boolean notStartsWith(Bound<T> valueExpr, Literal<T> lit) {
      return !startsWith(valueExpr, lit);
    }

    @Override
    public <T> Boolean stCoveredBy(Bound<T> valueExpr, Literal<T> lit) {
      Geometry geometry = evalAsGeometry(valueExpr);
      Geometry boundary = (Geometry) lit.to(GeometryType.get()).value();
      return geometry.coveredBy(boundary);
    }

    @Override
    public <T> Boolean stIntersects(Bound<T> valueExpr, Literal<T> lit) {
      Geometry geometry = evalAsGeometry(valueExpr);
      Geometry boundary = (Geometry) lit.to(GeometryType.get()).value();
      return geometry.intersects(boundary);
    }

    @Override
    public <T> Boolean stCovers(Bound<T> valueExpr, Literal<T> lit) {
      Geometry geometry = evalAsGeometry(valueExpr);
      Geometry boundary = (Geometry) lit.to(GeometryType.get()).value();
      return geometry.covers(boundary);
    }

    private <T> Boolean matchGeomPartition(Bound<T> valueExpr, IndexRangeSet rangeSet) {
      try {
        Long index = (Long) valueExpr.eval(struct);
        return rangeSet.match(index);
      } catch (Exception e) {
        LOG.warn(
            "Failed to eval geometry partition filter with `Bound`: {}; Error: {}", valueExpr, e);
        return true;
      }
    }

    @Override
    public <T> Boolean stCoveredBy(Bound<T> valueExpr, IndexRangeSet rangeSet) {
      return matchGeomPartition(valueExpr, rangeSet);
    }

    @Override
    public <T> Boolean stIntersects(Bound<T> valueExpr, IndexRangeSet rangeSet) {
      return matchGeomPartition(valueExpr, rangeSet);
    }

    @Override
    public <T> Boolean stCovers(Bound<T> valueExpr, IndexRangeSet rangeSet) {
      return matchGeomPartition(valueExpr, rangeSet);
    }

    private <T> Geometry evalAsGeometry(Bound<T> valueExpr) {
      Object value = valueExpr.eval(struct);
      if (value instanceof ByteBuffer) {
        return TypeUtil.GeometryUtils.byteBuffer2geometry((ByteBuffer) value);
      } else if (value instanceof Geometry) {
        return (Geometry) value;
      } else {
        throw new IllegalStateException(
            "left hand side of stCoveredBy operator is not geometry: " + value);
      }
    }
  }
}
