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
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/** a list of intervals representing a set of xz index. */
public class IndexRangeSet {
  private final List<Interval> intervalSet;
  public static final String INNER_DELIMITER = ",";
  public static final String OUTER_DELIMITER = ";";

  public enum IntervalLevel {
    // quadrant is in the query window
    WITHIN,
    // quadrant contains the query window
    CONTAINS,
    // quadrant is partially intersect with the query window
    PARTIAL_INTERSECT,
  }

  public IndexRangeSet(List<Interval> intervals) {
    this.intervalSet = mergeIntervals(intervals);
  }

  /**
   * Sort and merge intervals. For example, [(1, 2), (3, 4)] will be merged to [(1, 4)].
   *
   * @param intervals a list of intervals
   * @return return a list of sorted and merged intervals
   */
  public static List<Interval> mergeIntervals(List<Interval> intervals) {
    if (intervals.isEmpty()) {
      return intervals;
    }
    Collections.sort(intervals);
    List<Interval> mergedIntervals = Lists.newArrayList();
    Interval current = intervals.get(0);
    for (Interval interval : intervals) {
      if (current.getUpper() + 1 >= interval.getLower()) {
        IntervalLevel newLevel =
            current.getLevel().equals(interval.getLevel())
                ? current.getLevel()
                : IntervalLevel.PARTIAL_INTERSECT;
        current =
            new Interval(
                current.getLower(), Math.max(current.getUpper(), interval.getUpper()), newLevel);
      } else {
        mergedIntervals.add(current);
        current = interval;
      }
    }
    mergedIntervals.add(current);
    return mergedIntervals;
  }

  /**
   * the merged intervals
   *
   * @return list of intervals
   */
  public List<Interval> getIntervalSet() {
    return intervalSet;
  }

  @Override
  public String toString() {
    List<String> collect =
        intervalSet.stream().map(Interval::toString).collect(Collectors.toList());
    return String.join(OUTER_DELIMITER, collect);
  }

  /**
   * whether this interval set contains a specific index value
   *
   * @param idx value of xz index
   * @return true or false
   */
  public boolean match(long idx) {
    return matchInterval(idx, idx) != null;
  }

  public boolean match(long lower, long upper) {
    return matchInterval(lower, upper) != null;
  }

  /**
   * Find the specific interval which contains the given index, return null if not found
   *
   * @param lower min value of xz index
   * @param upper max value of xz index
   * @return return an Interval if found, otherwise return null
   */
  public Interval matchInterval(long lower, long upper) {
    int low = 0;
    int high = intervalSet.size() - 1;
    int mid = (low + high) / 2;
    while (low <= high) {
      Interval interval = intervalSet.get(mid);
      if (interval.match(lower, upper)) {
        return interval;
      }
      if (upper < interval.getLower()) {
        high = mid - 1;
      } else if (lower > interval.getUpper()) {
        low = mid + 1;
      }
      mid = (low + high) / 2;
    }
    return null;
  }

  /**
   * An "interval" is a range of xz index. For example, [1, 3] means this interval contains 1, 2, 3.
   */
  public static class Interval implements Comparable<Interval> {
    private final long lower;
    private final long upper;
    private final IntervalLevel level;

    Interval(long lower, long upper, IntervalLevel level) {
      this.lower = lower;
      this.upper = upper;
      this.level = level;
    }

    /**
     * the smallest matched index
     *
     * @return long
     */
    public long getLower() {
      return lower;
    }

    /**
     * the smallest matched index
     *
     * @return long
     */
    public long getUpper() {
      return upper;
    }

    /**
     * the relation between the rectangle(represent by the index) and the query window
     *
     * @return boolean
     */
    public IntervalLevel getLevel() {
      return level;
    }

    /**
     * return true if this interval contains the given index, false otherwise
     *
     * @param index xz index to be matched
     * @return boolean
     */
    public boolean match(long index) {
      return index >= lower && index <= upper;
    }

    /**
     * return true if this interval overlapped with the given bounds, false otherwise
     *
     * @param lowerBound the smallest xz2 index
     * @param upperBound the largest xz2 index
     * @return boolean
     */
    public boolean match(long lowerBound, long upperBound) {
      return lowerBound <= upper && upperBound >= lower;
    }

    @Override
    public String toString() {
      return lower + INNER_DELIMITER + upper + INNER_DELIMITER + level;
    }

    /**
     * compare the intervals by its `lower` value, will use this feature to sort and merge intervals
     *
     * @param that interval to be compared
     * @return an integer
     */
    @Override
    public int compareTo(Interval that) {
      return Long.compare(this.lower, that.lower);
    }
  }
}
