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

import static org.apache.iceberg.types.Conversions.toByteBuffer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

public class TestGeometryHelpers {
  private static final GeometryFactory GF = new GeometryFactory();
  private static final double EPSILON = 1e-5;

  public static class ManifestEvalData {
    // when resolution=2, the xz2 index interval [17, 21] represents the top right quadrant,
    // in which the values are non-negative
    public static final int RESOLUTION = 2;
    public static final long XZ2_MIN_VALUE = 17L;
    public static final long XZ2_MAX_VALUE = 21L;
    public static final ByteBuffer GEOM_INDEX_MIN =
        toByteBuffer(Types.LongType.get(), XZ2_MIN_VALUE);
    public static final ByteBuffer GEOM_INDEX_MAX =
        toByteBuffer(Types.LongType.get(), XZ2_MAX_VALUE);
    public static final List<Geometry> SLIDING_WINDOWS = generateWindows();

    private static List<Geometry> generateWindows() {
      List<Geometry> slidingWindows = Lists.newArrayList();
      int width = 30;
      int xVal = -180;
      while (xVal < 180) {
        int yVal = -90;
        while (yVal < 90) {
          slidingWindows.add(GF.toGeometry(new Envelope(xVal, xVal + width, yVal, yVal + width)));
          yVal += width;
        }
        xVal += width;
      }
      return slidingWindows;
    }

    /**
     * Generate points that match the xz2 index interval [XZ2_MIN_VALUE, XZ2_MAX_VALUE]
     *
     * @param number number of random times
     * @return array of points
     */
    public static Point[] generateInsidePoints(int number) {
      Random random = new Random();
      Point[] insidePoints = new Point[number * 3];
      for (int i = 0; i < number; i++) {
        int xVal = random.nextInt(180) + 1;
        int yVal = random.nextInt(90) + 1;
        insidePoints[3 * i] = GF.createPoint(new Coordinate(xVal, yVal));
        insidePoints[3 * i + 1] = GF.createPoint(new Coordinate(0, yVal));
        insidePoints[3 * i + 2] = GF.createPoint(new Coordinate(xVal, 0));
      }
      return insidePoints;
    }

    /**
     * Generate points that not match the xz2 index interval [XZ2_MIN_VALUE, XZ2_MAX_VALUE]
     *
     * @param number number of random times
     * @return array of points
     */
    public static Point[] generateOutsidePoints(int number) {
      Random random = new Random();
      Point[] outsidePoints = new Point[number * 3];
      for (int i = 0; i < number; i++) {
        int xVal = random.nextInt(180) + 1;
        int yVal = random.nextInt(90) + 1;
        outsidePoints[3 * i] = GF.createPoint(new Coordinate(-xVal, yVal));
        outsidePoints[3 * i + 1] = GF.createPoint(new Coordinate(xVal, -yVal));
        outsidePoints[3 * i + 2] = GF.createPoint(new Coordinate(-xVal, -yVal));
      }
      return outsidePoints;
    }
  }

  public static class MetricEvalData {
    public static final double GEOM_X_MIN = -10.0;
    public static final double GEOM_Y_MIN = -1.0;
    public static final double GEOM_X_MAX = 10.0;
    public static final double GEOM_Y_MAX = 1.0;
    public static final Envelope GEOM_METRIC_WINDOW =
        new Envelope(GEOM_X_MIN, GEOM_X_MAX, GEOM_Y_MIN, GEOM_Y_MAX);

    public static final Geometry[] TOUCHED_WINDOWS =
        new Geometry[] {
          // on the left
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MIN, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1),
          // on the bottom
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MIN),
          // on the top
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MAX, GEOM_Y_MAX + 1),
          // on the right
          generateGeom(GEOM_X_MAX, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1)
        };

    public static final Geometry[] TOUCHED_POINTS =
        new Geometry[] {
          generateGeom(GEOM_X_MIN, GEOM_X_MIN, GEOM_Y_MIN, GEOM_Y_MIN),
          generateGeom(GEOM_X_MIN, GEOM_X_MIN, GEOM_Y_MAX, GEOM_Y_MAX),
          generateGeom(GEOM_X_MAX, GEOM_X_MAX, GEOM_Y_MIN, GEOM_Y_MIN),
          generateGeom(GEOM_X_MAX, GEOM_X_MAX, GEOM_Y_MAX, GEOM_Y_MAX),
          // point in the metric window
          generateGeom(
              GEOM_X_MIN + EPSILON,
              GEOM_X_MIN + EPSILON,
              GEOM_Y_MIN + EPSILON,
              GEOM_Y_MIN + EPSILON)
        };

    public static final Geometry[] TOUCHED_LINES =
        new Geometry[] {
          generateGeom(GEOM_X_MIN, GEOM_X_MIN, GEOM_Y_MIN, GEOM_Y_MAX),
          generateGeom(GEOM_X_MAX, GEOM_X_MAX, GEOM_Y_MIN, GEOM_Y_MAX),
          generateGeom(GEOM_X_MIN, GEOM_X_MAX, GEOM_Y_MIN, GEOM_Y_MIN),
          generateGeom(GEOM_X_MIN, GEOM_X_MAX, GEOM_Y_MAX, GEOM_Y_MAX),
          // lines in the metric window
          generateGeom(GEOM_X_MIN + EPSILON, GEOM_X_MIN + EPSILON, GEOM_Y_MIN, GEOM_Y_MAX),
          generateGeom(GEOM_X_MIN, GEOM_X_MAX, GEOM_Y_MIN + EPSILON, GEOM_Y_MIN + EPSILON),
        };

    public static final Geometry[] INSIDE_WINDOWS =
        new Geometry[] {
          generateGeom(GEOM_X_MIN, GEOM_X_MIN + EPSILON, GEOM_Y_MIN, GEOM_Y_MIN + EPSILON),
          generateGeom(GEOM_X_MIN, GEOM_X_MAX - EPSILON, GEOM_Y_MIN, GEOM_Y_MIN + EPSILON),
          generateGeom(GEOM_X_MIN, GEOM_X_MAX - EPSILON, GEOM_Y_MIN + EPSILON, GEOM_Y_MAX),
          generateGeom(GEOM_X_MIN, GEOM_X_MIN + EPSILON, GEOM_Y_MIN, GEOM_Y_MAX - EPSILON),
          generateGeom(
              GEOM_X_MIN + EPSILON,
              GEOM_X_MAX - EPSILON,
              GEOM_Y_MIN + EPSILON,
              GEOM_Y_MAX - EPSILON)
        };

    public static final Geometry[] DISJOINT_WINDOWS =
        new Geometry[] {
          // on the left
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MIN - EPSILON, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1),
          // on the bottom
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MIN - EPSILON),
          // on the top
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MAX + EPSILON, GEOM_Y_MAX + 1),
          // on the right
          generateGeom(GEOM_X_MAX + EPSILON, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1)
        };

    public static final Geometry[] OVERLAPPED_BUT_NOT_INSIDE_WINDOWS =
        new Geometry[] {
          // on the left
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MIN + EPSILON, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1),
          // on the bottom
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MIN + EPSILON),
          // bottom left
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MIN + EPSILON, GEOM_Y_MIN - 1, GEOM_Y_MIN + EPSILON),
          // on the top
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MAX + 1, GEOM_Y_MAX - EPSILON, GEOM_Y_MAX + 1),
          // top left
          generateGeom(GEOM_X_MIN - 1, GEOM_X_MIN + EPSILON, GEOM_Y_MAX - EPSILON, GEOM_Y_MAX + 1),

          // on the right
          generateGeom(GEOM_X_MAX - EPSILON, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MAX + 1),
          // bottom right
          generateGeom(GEOM_X_MAX - EPSILON, GEOM_X_MAX + 1, GEOM_Y_MIN - 1, GEOM_Y_MIN + EPSILON),
          // top right
          generateGeom(GEOM_X_MAX - EPSILON, GEOM_X_MAX + 1, GEOM_Y_MAX - EPSILON, GEOM_Y_MAX + 1),
          // contains the metric window
          generateGeom(
              GEOM_X_MIN - EPSILON,
              GEOM_X_MAX + EPSILON,
              GEOM_Y_MIN - EPSILON,
              GEOM_Y_MAX + EPSILON)
        };

    private static Geometry generateGeom(double xmin, double xmax, double ymin, double ymax) {
      Envelope envelope = new Envelope(xmin, xmax, ymin, ymax);
      return GF.toGeometry(envelope);
    }
  }

  public static class RecordEvalData {
    public static final Point BASE_POINT = GF.createPoint(new Coordinate(0, 0));
    public static final LineString BASE_LINE_STRING =
        GF.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)});
    public static final Geometry BASE_POLYGON = GF.toGeometry(new Envelope(0, 1, 0, 1));

    public static final Geometry[] DISJOINT_WITH_BASE_POINT =
        new Geometry[] {
          GF.createPoint(new Coordinate(EPSILON, 0)),
          GF.createPoint(new Coordinate(0, EPSILON)),
          GF.createLineString(
              new Coordinate[] {new Coordinate(EPSILON, EPSILON), new Coordinate(1, 1)}),
          GF.toGeometry(new Envelope(EPSILON, 1, EPSILON, 1))
        };

    public static final Geometry[] GEOM_CONTAINS_BASE_POINT =
        new Geometry[] {BASE_LINE_STRING, BASE_POLYGON};

    // for line string
    public static final Geometry[] DISJOINT_WITH_BASE_LINESTRING =
        new Geometry[] {
          GF.createPoint(new Coordinate(-1, -1)),
          GF.createPoint(new Coordinate(0, 1)),
          GF.createPoint(new Coordinate(1, 0)),
          GF.createLineString(
              new Coordinate[] {new Coordinate(0, EPSILON), new Coordinate(1, 1 + EPSILON)}),
          GF.toGeometry(new Envelope(1 + EPSILON, 1 + 2 * EPSILON, 1 + EPSILON, 1 + 2 * EPSILON))
        };

    public static final Geometry[] ONLY_INTERSECT_WITH_BASE_LINESTRING =
        new Geometry[] {
          GF.createLineString(new Coordinate[] {new Coordinate(0, 0), new Coordinate(0, 1)}),
          GF.createLineString(new Coordinate[] {new Coordinate(1, 0), new Coordinate(1, 1)}),
          GF.createLineString(
              new Coordinate[] {new Coordinate(0, 1), new Coordinate(EPSILON, EPSILON)}),
          GF.toGeometry(new Envelope(0, EPSILON, 0, EPSILON)),
          GF.toGeometry(new Envelope(EPSILON, 1, EPSILON, 1))
        };

    public static final Geometry[] COVERED_BY_BASE_LINESTRING =
        new Geometry[] {
          BASE_POINT,
          GF.createPoint(new Coordinate(EPSILON, EPSILON)),
          GF.createLineString(
              new Coordinate[] {new Coordinate(0, 0), new Coordinate(EPSILON, EPSILON)}),
          GF.createLineString(
              new Coordinate[] {new Coordinate(EPSILON, EPSILON), new Coordinate(1, 1)})
        };

    public static final Geometry[] COVERS_BASE_LINESTRING =
        new Geometry[] {
          GF.createLineString(
              new Coordinate[] {new Coordinate(-EPSILON, -EPSILON), new Coordinate(1, 1)}),
          GF.createLineString(
              new Coordinate[] {new Coordinate(0, 0), new Coordinate(1 + EPSILON, 1 + EPSILON)}),
          GF.toGeometry(new Envelope(-EPSILON, 1, -EPSILON, 1)),
          GF.toGeometry(new Envelope(0, 1 + EPSILON, 0, 1 + EPSILON))
        };

    public static final Geometry[] DISJOINT_WITH_BASE_POLYGON =
        new Geometry[] {
          GF.createPoint(new Coordinate(-EPSILON, -EPSILON)),
          GF.createLineString(
              new Coordinate[] {new Coordinate(-EPSILON, -EPSILON), new Coordinate(0, -EPSILON)}),
          GF.toGeometry(new Envelope(-EPSILON, -EPSILON / 2, -EPSILON, -EPSILON / 2))
        };

    public static final Geometry[] ONLY_INTERSECT_WITH_BASE_POLYGON =
        new Geometry[] {
          GF.createLineString(
              new Coordinate[] {new Coordinate(-EPSILON, -EPSILON), new Coordinate(1, 1)}),
          GF.toGeometry(new Envelope(-EPSILON, 0, -EPSILON, 0)),
          GF.toGeometry(new Envelope(-EPSILON, EPSILON, -EPSILON, EPSILON)),
        };

    public static final Geometry[] COVERED_BY_BASE_POLYGON =
        new Geometry[] {
          BASE_POINT, BASE_LINE_STRING, GF.toGeometry(new Envelope(0, EPSILON, 0, EPSILON))
        };

    public static final Geometry[] COVERS_BASE_POLYGON =
        new Geometry[] {
          GF.toGeometry(new Envelope(0, 1 + EPSILON, 0, 1 + EPSILON)),
          GF.toGeometry(new Envelope(-EPSILON, 1, -EPSILON, 1)),
          GF.toGeometry(new Envelope(-EPSILON, 1, 0, 1)),
          GF.toGeometry(new Envelope(0, 1, -EPSILON, 1))
        };
  }

  @FunctionalInterface
  public interface TriFunction<A, B, C, R> {

    R apply(A arg1, B arg2, C arg3);

    default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
      Preconditions.checkNotNull(after, "function can not be null");
      return (A arg1, B arg2, C arg3) -> after.apply(apply(arg1, arg2, arg3));
    }
  }

  public static class StExpressionTester<S, T> {
    private List<T> matchedQuery = Lists.newArrayList();
    private List<T> unmatchedQuery = Lists.newArrayList();
    private S data;
    private Expression.Operation op;
    private TriFunction<Expression.Operation, S, T, Boolean> evaluator;

    public StExpressionTester<S, T> resetQuery() {
      this.matchedQuery = Lists.newArrayList();
      this.unmatchedQuery = Lists.newArrayList();
      return this;
    }

    public StExpressionTester<S, T> withOperation(Expression.Operation operator) {
      this.op = operator;
      return this;
    }

    public StExpressionTester<S, T> withMatchedQuery(T query) {
      this.matchedQuery.add(query);
      return this;
    }

    public StExpressionTester<S, T> withMatchedQuery(T[] query) {
      this.matchedQuery.addAll(Arrays.asList(query));
      return this;
    }

    public StExpressionTester<S, T> withUnmatchedQuery(T query) {
      this.unmatchedQuery.add(query);
      return this;
    }

    public StExpressionTester<S, T> withUnmatchedQuery(T[] query) {
      this.unmatchedQuery.addAll(Arrays.asList(query));
      return this;
    }

    public StExpressionTester<S, T> withData(S baseData) {
      this.data = baseData;
      return this;
    }

    public StExpressionTester<S, T> withEvaluator(
        TriFunction<Expression.Operation, S, T, Boolean> eval) {
      this.evaluator = eval;
      return this;
    }

    public void run() {
      this.matchedQuery.forEach(
          query -> {
            String hint = String.format("%s should %s %s", data, op.name(), query);
            Assert.assertEquals(hint, true, evaluator.apply(op, data, query));
          });
      this.unmatchedQuery.forEach(
          query -> {
            String hint = String.format("%s should not %s %s", data, op.name(), query);
            Assert.assertEquals(hint, false, evaluator.apply(op, data, query));
          });
    }
  }
}
