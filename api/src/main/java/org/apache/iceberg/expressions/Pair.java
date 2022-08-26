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

import org.apache.iceberg.relocated.com.google.common.base.Objects;

/**
 * Represents a pair of values of type T0 and T1
 *
 * @param <T0> type of first value
 * @param <T1> type of second value
 */
public class Pair<T0, T1> {
  private T0 first;
  private T1 second;

  public static <T0, T1> Pair<T0, T1> of(T0 first, T1 second) {
    return new Pair(first, second);
  }

  private Pair(T0 first, T1 second) {
    this.first = first;
    this.second = second;
  }

  public T0 first() {
    return first;
  }

  public T1 second() {
    return second;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(first, second);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (!(other instanceof Pair)) {
      return false;
    }
    Pair<?, ?> otherPair = (Pair<?, ?>) other;
    return Objects.equal(first, otherPair.first) && Objects.equal(second, otherPair.second);
  }

  @Override
  public String toString() {
    return String.format("pair<%s, %s>", first, second);
  }
}
