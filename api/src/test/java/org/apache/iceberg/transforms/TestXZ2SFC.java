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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.transforms.geometry.IndexRangeSet;
import org.apache.iceberg.transforms.geometry.XZ2SFCurving;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class TestXZ2SFC {
  @Test
  public void testXZ2Index() {
    int resolution = 12;
    XZ2SFCurving sfc = new XZ2SFCurving(resolution);
    Assert.assertEquals(9317037L, sfc.index(0.0, -1.0, 7.5, 0.0));
    Assert.assertEquals(16841390L, sfc.index(10.0, 10.0, 12.0, 12.0));
  }

  @Test
  public void testWholeRangeQuery() {
    int maxResolution = 16;
    for (int i = 2; i <= maxResolution; i++) {
      XZ2SFCurving sfc = new XZ2SFCurving(i);
      List<IndexRangeSet.Interval> intervalSet = sfc.ranges(-180, -90, 180, 90);
      IndexRangeSet indexRangeSet = new IndexRangeSet(intervalSet);
      intervalSet = indexRangeSet.getIntervalSet();
      // should return a single interval that covers all the indexes
      Assert.assertEquals(1, intervalSet.size());
      IndexRangeSet.Interval interval = intervalSet.get(0);
      Assert.assertEquals(1L, interval.getLower());
      Assert.assertEquals((long) (Math.pow(4, i + 1) - 1) / 3, interval.getUpper());
      Assert.assertTrue(interval.getLevel().equals(IndexRangeSet.IntervalLevel.WITHIN));
    }
  }

  @Test
  public void testXZ2ranges() {
    int resolution = 12;
    XZ2SFCurving sfc = new XZ2SFCurving(resolution);
    long index = sfc.index(10.0, 10.0, 12.0, 12.0);
    Assert.assertEquals(16841390L, index);
    IndexRangeSet indexRangeSet;

    // test contains
    List<XZ2SFCurving.Bound> containing =
        Arrays.asList(
            new XZ2SFCurving.Bound(9.0, 9.0, 13.0, 13.0),
            new XZ2SFCurving.Bound(-180.0, -90.0, 180.0, 90.0),
            new XZ2SFCurving.Bound(0.0, 0.0, 180.0, 90.0),
            new XZ2SFCurving.Bound(0.0, 0.0, 20.0, 20.0));
    for (XZ2SFCurving.Bound bound : containing) {
      List<IndexRangeSet.Interval> intervalSet =
          sfc.ranges(bound.getxMin(), bound.getyMin(), bound.getxMax(), bound.getyMax());
      indexRangeSet = new IndexRangeSet(intervalSet);
      Assert.assertTrue(indexRangeSet.match(index));
    }

    // test overlapping
    List<XZ2SFCurving.Bound> overlapping =
        Arrays.asList(
            new XZ2SFCurving.Bound(11.0, 11.0, 13.0, 13.0),
            new XZ2SFCurving.Bound(9.0, 9.0, 11.0, 11.0),
            new XZ2SFCurving.Bound(10.5, 10.5, 11.5, 11.5),
            new XZ2SFCurving.Bound(11.0, 11.0, 11.0, 11.0));
    for (XZ2SFCurving.Bound bound : overlapping) {
      List<IndexRangeSet.Interval> intervalSet =
          sfc.ranges(bound.getxMin(), bound.getyMin(), bound.getxMax(), bound.getyMax());
      indexRangeSet = new IndexRangeSet(intervalSet);
      Assert.assertTrue(indexRangeSet.match(index));
    }

    // test disjoint
    List<XZ2SFCurving.Bound> disjoint =
        Arrays.asList(
            new XZ2SFCurving.Bound(-180.0, -90.0, 8.0, 8.0),
            new XZ2SFCurving.Bound(0.0, 0.0, 8.0, 8.0),
            new XZ2SFCurving.Bound(9.0, 9.0, 9.5, 9.5),
            new XZ2SFCurving.Bound(20.0, 20.0, 180.0, 90.0));
    for (XZ2SFCurving.Bound bound : disjoint) {
      List<IndexRangeSet.Interval> intervalSet =
          sfc.ranges(bound.getxMin(), bound.getyMin(), bound.getxMax(), bound.getyMax());
      indexRangeSet = new IndexRangeSet(intervalSet);
      Assert.assertFalse(indexRangeSet.match(index));
    }
  }

  @Test
  public void testRangeByGeometry() {
    int resolution = 12;
    XZ2SFCurving sfc = new XZ2SFCurving(resolution);
    double xMin = -1;
    double xMax = 1;
    double yMin = -1;
    double yMax = 1;

    // when the query window is a rectangle parallel to the axes, two `ranges` methods should return
    // the same result
    Envelope envelope = new Envelope(xMin, xMax, yMin, yMax);
    Geometry geometry = new GeometryFactory().toGeometry(envelope);
    IndexRangeSet rangesByGeom = new IndexRangeSet(sfc.ranges(geometry));

    IndexRangeSet rangesByBound = new IndexRangeSet(sfc.ranges(xMin, yMin, xMax, yMax));
    Assert.assertEquals(rangesByGeom.toString(), rangesByBound.toString());

    // otherwise, they can not return the same result
    Point point = new GeometryFactory().createPoint(new Coordinate(0, 0));
    geometry = point.buffer(10);
    envelope = geometry.getEnvelopeInternal();
    xMin = envelope.getMinX();
    xMax = envelope.getMaxX();
    yMin = envelope.getMinY();
    yMax = envelope.getMaxY();
    rangesByGeom = new IndexRangeSet(sfc.ranges(geometry));
    rangesByBound = new IndexRangeSet(sfc.ranges(xMin, yMin, xMax, yMax));
    Assert.assertNotEquals(rangesByGeom.toString(), rangesByBound.toString());
  }
}
