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

import java.util.Comparator;

public class MinAggregator<T> extends BoundAggregate.NullSafeAggregator<T, T> {
  private final Comparator<T> comparator;
  private T min = null;

  MinAggregator(ValueAggregate<T> aggregate, Comparator<T> comparator) {
    super(aggregate);
    this.comparator = comparator;
  }

  @Override
  protected void update(T value) {
    if (min == null) {
      this.min = value;
    } else if (value instanceof Pair && min instanceof Pair) {
      try {
        // this only happens when the value is a Pair<Double, Double> (bounds of a geometry)
        Pair<Double, Double> valuePair = (Pair<Double, Double>) value;
        Pair<Double, Double> minPair = (Pair<Double, Double>) min;
        this.min =
            (T)
                Pair.of(
                    Math.min(valuePair.first(), minPair.first()),
                    Math.min(valuePair.second(), minPair.second()));
      } catch (ClassCastException e) {
        throw new UnsupportedOperationException(
            "MinAggregator only supports Pair<Double, Double> values");
      }
    } else if (comparator.compare(value, min) < 0) {
      this.min = value;
    }
  }

  @Override
  protected T current() {
    return min;
  }
}
