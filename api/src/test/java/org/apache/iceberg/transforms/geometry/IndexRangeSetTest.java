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

import java.util.Collections;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class IndexRangeSetTest {
  @Test
  public void testInterval() {
    int bound = 10;
    int lower = 3;
    int upper = 6;
    IndexRangeSet.Interval interval =
        new IndexRangeSet.Interval(lower, upper, IndexRangeSet.IntervalLevel.WITHIN);
    Assert.assertTrue(interval.match(lower));
    Assert.assertTrue(interval.match(upper));
    Assert.assertFalse(interval.match(lower - 1));
    Assert.assertFalse(interval.match(upper + 1));

    for (int lowerBound = 0; lowerBound < bound; lowerBound++) {
      for (int upperBound = lowerBound; upperBound < bound; upperBound++) {
        Assert.assertEquals(
            lowerBound > upper || upperBound < lower, !interval.match(lowerBound, upperBound));
      }
    }
  }

  @Test
  public void testSortInterval() {
    IndexRangeSet.IntervalLevel level = IndexRangeSet.IntervalLevel.CONTAINS;
    List<IndexRangeSet.Interval> intervals = Lists.newArrayList();
    intervals.add(new IndexRangeSet.Interval(3, 4, level));
    intervals.add(new IndexRangeSet.Interval(1, 2, level));

    Collections.sort(intervals);
    Assert.assertEquals(intervals.get(0).getLower(), 1);
    Assert.assertEquals(intervals.get(0).getUpper(), 2);
    Assert.assertEquals(intervals.get(1).getLower(), 3);
    Assert.assertEquals(intervals.get(1).getUpper(), 4);

    IndexRangeSet indexRangeSet = new IndexRangeSet(intervals);
    Assert.assertEquals(indexRangeSet.getIntervalSet().size(), 1);
    IndexRangeSet.Interval interval = indexRangeSet.getIntervalSet().get(0);
    Assert.assertEquals(interval.getLower(), 1);
    Assert.assertEquals(interval.getUpper(), 4);
  }
}
