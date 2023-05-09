/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.expressions;

import java.util.Comparator;

public class MaxAggregator<T> extends BoundAggregate.NullSafeAggregator<T, T> {
  private final Comparator<T> comparator;
  private T max = null;

  MaxAggregator(ValueAggregate<T> aggregate, Comparator<T> comparator) {
    super(aggregate);
    this.comparator = comparator;
  }

  @Override
  protected void update(T value) {
    if (max == null) {
      this.max = value;
    } else if (value instanceof Pair && max instanceof Pair) {
      try {
        // this only happens when the value is a Pair<Double, Double> (bounds of a geometry)
        Pair<Double, Double> valuePair = (Pair<Double, Double>) value;
        Pair<Double, Double> maxPair = (Pair<Double, Double>) max;
        this.max = (T) Pair.of(Math.max(valuePair.first(), maxPair.first()),
          Math.max(valuePair.second(), maxPair.second()));
      } catch (ClassCastException e) {
        throw new UnsupportedOperationException("MaxAggregator only supports Pair<Double, Double> values");
      }
    } else if (comparator.compare(value, max) > 0) {
      this.max = value;
    }
  }

  @Override
  protected T current() {
    return max;
  }
}
