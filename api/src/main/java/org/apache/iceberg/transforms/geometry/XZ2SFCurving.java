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
package org.apache.iceberg.transforms.geometry;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class XZ2SFCurving {
  private final int resolution;
  private static final double LogPointFive = Math.log(0.5);
  private static final Bound GeoBound = new Bound(-180, -90, 180, 90);
  private static final Bound NormalizeBound = new Bound(0, 0, 1, 1);
  private static final List<XElement> LevelOneElements =
      new XElement(0.0, 0.0, 1.0, 1.0, 1.0).children();
  private static final XElement LevelTerminator = new XElement(-1.0, -1.0, -1.0, -1.0, 0);

  public XZ2SFCurving(int resolution) {
    this.resolution = resolution;
  }

  /**
   * return the index of xz2 order
   *
   * @param xMin minimum value on x axis
   * @param yMin minimum value on y axis
   * @param xMax maximum value on x axis
   * @param yMax maximum value on y axis
   * @return long
   */
  public long index(double xMin, double yMin, double xMax, double yMax) {
    return index(new Bound(xMin, yMin, xMax, yMax));
  }

  /**
   * return a list of interval set
   *
   * @param xMin minimum value on x axis
   * @param yMin minimum value on y axis
   * @param xMax maximum value on x axis
   * @param yMax maximum value on y axis
   * @param strict whether return the overlapped range set
   * @return IndexRangeSet
   */
  public IndexRangeSet ranges(double xMin, double yMin, double xMax, double yMax, boolean strict) {
    Bound bound = normalize(new Bound(xMin, yMin, xMax, yMax));
    ArrayDeque<XElement> remaining = new ArrayDeque<>(LevelOneElements);
    remaining.addLast(LevelTerminator);
    List<IndexRangeSet.Interval> intervals = Lists.newArrayList();
    int level = 1;
    while (!remaining.isEmpty() && level < resolution) {
      XElement next = remaining.pollFirst();
      if (next.equals(LevelTerminator)) {
        if (!remaining.isEmpty()) {
          level += 1;
          remaining.addLast(LevelTerminator);
        }
      } else {
        if (next.isContained(bound)) {
          // whole range in the bounding box
          Long[] indexes = sequenceInterval(next.xMin, next.yMin, level, false);
          intervals.add(new IndexRangeSet.Interval(indexes[0], indexes[1], true));
        } else if (next.overlaps(bound)) {
          Long[] indexes = sequenceInterval(next.xMin, next.yMin, level, true);
          // add overlapped interval if not in "strict" mode
          if (!strict) {
            intervals.add(new IndexRangeSet.Interval(indexes[0], indexes[1], false));
          }
          remaining.addAll(next.children());
        }
      }
    }
    while (!remaining.isEmpty()) {
      XElement next = remaining.pollFirst();
      if (next.equals(LevelTerminator)) {
        level += 1;
      } else {
        Long[] indexes = sequenceInterval(next.xMin, next.yMin, level, false);
        if (strict) {
          if (next.isContained(bound)) {
            intervals.add(new IndexRangeSet.Interval(indexes[0], indexes[1], true));
          }
        } else {
          intervals.add(new IndexRangeSet.Interval(indexes[0], indexes[1], false));
        }
      }
    }
    return new IndexRangeSet(intervals);
  }

  private long index(Bound bound) {
    final Bound normBound = normalize(bound);
    double maxDim = Math.max(normBound.xMax - normBound.xMin, normBound.yMax - normBound.yMin);
    final int l1 = (int) Math.floor(Math.log(maxDim) / LogPointFive);
    int length = resolution;
    // the length will either be (l1) or (l1 + 1)
    if (l1 < resolution) {
      double w2 = Math.pow(0.5, l1 + 1); // width of an element at resolution l2 (l1 + 1)
      if (predicate(normBound.xMin, normBound.xMax, w2)
          && predicate(normBound.yMin, normBound.yMax, w2)) {
        length = l1 + 1;
      }
    }
    return sequenceCode(normBound.xMin, normBound.yMin, length);
  }

  // predicate for checking how many axis the polygon intersects
  // math.floor(min / w2) * w2 == start of cell containing min
  private boolean predicate(final double min, final double max, final double w2) {
    return max <= ((Math.floor(min / w2) * w2) + (2 * w2));
  }

  private Long[] sequenceInterval(double xVal, double yVal, int length, boolean partial) {
    Long minVal = sequenceCode(xVal, yVal, length);
    Long maxVal = partial ? minVal : minVal + elements(length - 1);
    return new Long[] {minVal, maxVal};
  }

  private long elements(int level) {
    return (long) ((Math.pow(4, resolution - level) - 1) / 3);
  }

  private Long sequenceCode(double xVal, double yVal, int length) {
    double xMin = NormalizeBound.xMin;
    double yMin = NormalizeBound.yMin;
    double xMax = NormalizeBound.xMax;
    double yMax = NormalizeBound.yMax;
    long idx = 0;
    int precision = 0;
    while (precision < length) {
      double xCenter = (xMin + xMax) / 2;
      double yCenter = (yMin + yMax) / 2;
      if (xVal < xCenter) {
        xMax = xCenter;
        if (yVal < yCenter) {
          idx++;
          yMax = yCenter;
        } else {
          idx += 1 + 2 * elements(precision);
          yMin = yCenter;
        }
      } else {
        xMin = xCenter;
        if (yVal < yCenter) {
          idx += 1 + elements(precision);
          yMax = yCenter;
        } else {
          idx += 1 + 3 * elements(precision);
          yMin = yCenter;
        }
      }
      precision++;
    }
    return idx;
  }

  public static class Bound {
    private final double xMin;
    private final double xMax;
    private final double yMin;
    private final double yMax;

    public Bound(double x1, double y1, double x2, double y2) {
      this.xMin = x1;
      this.xMax = x2;
      this.yMin = y1;
      this.yMax = y2;
    }

    public double getxMin() {
      return xMin;
    }

    public double getxMax() {
      return xMax;
    }

    public double getyMin() {
      return yMin;
    }

    public double getyMax() {
      return yMax;
    }

    @Override
    public String toString() {
      return String.format("(xMin: %s; xMax: %s; yMin: %s, yMax: %s)", xMin, xMax, yMin, yMax);
    }
  }

  private Bound normalize(Bound bound) {
    if (bound.xMin < GeoBound.xMin
        || bound.xMax > GeoBound.xMax
        || bound.yMin < GeoBound.xMin
        || bound.yMax > GeoBound.yMax) {
      throw new IllegalArgumentException("Invalid bound: " + bound);
    }
    double xMin = (bound.xMin - GeoBound.xMin) / (GeoBound.xMax - GeoBound.xMin);
    double xMax = (bound.xMax - GeoBound.xMin) / (GeoBound.xMax - GeoBound.xMin);
    double yMin = (bound.yMin - GeoBound.yMin) / (GeoBound.yMax - GeoBound.yMin);
    double yMax = (bound.yMax - GeoBound.yMin) / (GeoBound.yMax - GeoBound.yMin);
    return new Bound(xMin, yMin, xMax, yMax);
  }

  /**
   * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of
   * calculation.
   *
   * <p>An extended Z element refers to a normal Z curve element that has its upper bounds expanded
   * by double its dimensions. By convention, an element is always an n-cube.
   */
  private static class XElement {
    private final double xMin;
    private final double yMin;
    private final double xMax;
    private final double yMax;
    private final double length;
    private final double xExt;
    private final double yExt;

    XElement(double xMin, double yMin, double xMax, double yMax, double len) {
      this.xMin = xMin;
      this.xMax = xMax;
      this.yMin = yMin;
      this.yMax = yMax;
      this.length = len;
      this.xExt = Math.min(xMax + length, NormalizeBound.xMax);
      this.yExt = Math.min(yMax + length, NormalizeBound.yMax);
    }

    public boolean isContained(Bound window) {
      return window.xMin <= xMin
          && window.xMax >= xExt
          && window.yMin <= yMin
          && window.yMax >= yExt;
    }

    public boolean overlaps(Bound window) {
      return window.xMax >= xMin
          && window.yMax >= yMin
          && window.xMin <= xExt
          && window.yMin <= yExt;
    }

    public List<XElement> children() {
      double xCenter = (xMax + xMin) / 2;
      double yCenter = (yMax + yMin) / 2;
      double len = length / 2;
      return Arrays.asList(
          new XElement(xMin, yMin, xCenter, yCenter, len),
          new XElement(xCenter, yMin, xMax, yCenter, len),
          new XElement(xMin, yCenter, xCenter, yMax, len),
          new XElement(xCenter, yCenter, xMax, yMax, len));
    }

    @Override
    public String toString() {
      return String.format("([%s, %s], [%s, %s], len=%s)", xMin, xMax, yMin, yMax, length);
    }
  }
}
