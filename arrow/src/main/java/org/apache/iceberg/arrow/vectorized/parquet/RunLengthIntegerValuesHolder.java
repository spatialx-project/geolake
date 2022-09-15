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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.util.Arrays;

/** Holder of run-length encoding integer values such as repetition levels or definition levels. */
public class RunLengthIntegerValuesHolder {

  private int[] values;
  private int[] runLengths;
  private int capacity;
  private int length;

  public RunLengthIntegerValuesHolder(int initialCapacity) {
    this.capacity = initialCapacity <= 0 ? 16 : initialCapacity;
    this.values = new int[this.capacity];
    this.runLengths = new int[this.capacity];
    this.length = 0;
  }

  public RunLengthIntegerValuesHolder() {
    this(16);
  }

  public int getLength() {
    return length;
  }

  public int getCount() {
    int count = 0;
    for (int k = 0; k < length; k++) {
      count += runLengths[k];
    }
    return count;
  }

  public void setCapacity(int newCapacity) {
    if (newCapacity > capacity) {
      values = Arrays.copyOf(values, newCapacity);
      runLengths = Arrays.copyOf(runLengths, newCapacity);
      capacity = newCapacity;
    }
  }

  public int getValue(int index) {
    return values[index];
  }

  public int getRunLength(int index) {
    return runLengths[index];
  }

  public void clear() {
    this.length = 0;
  }

  public void add(int value, int runLength) {
    if (length > 0 && values[length - 1] == value) {
      runLengths[length - 1] += runLength;
    } else {
      if (length == capacity) {
        setCapacity(capacity * 2);
      }
      values[length] = value;
      runLengths[length] = runLength;
      length += 1;
    }
  }
}
