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

public final class VectorizedParquetRepetitionLevelReader
    extends BaseVectorizedParquetValuesReader {

  private final int triplesCount;
  private int triplesRead = 0;

  public VectorizedParquetRepetitionLevelReader(int bitWidth, int triplesCount) {
    this(bitWidth, bitWidth != 0, triplesCount);
  }

  public VectorizedParquetRepetitionLevelReader(
      int bitWidth, boolean readLength, int triplesCount) {
    super(bitWidth, 0, readLength, false);
    this.triplesCount = triplesCount;
  }

  /**
   * Read repetition level for next N records. Repetition level = 0 designates the beginning of a
   * record.
   *
   * @param maxRecords maximum number of records the repetition levels spans
   * @param runLengthValues output parameter for receiving run-length values
   * @return actual number of records covered by repetition levels
   */
  public int readRepetitionLevels(int maxRecords, RunLengthIntegerValuesHolder runLengthValues) {
    int numRecords = 0;
    while (triplesRead < triplesCount) {
      if (currentCount == 0) {
        readNextGroup();
      }

      switch (mode) {
        case RLE:
          if (currentValue == 0) {
            if (numRecords + currentCount > maxRecords) {
              int consumedRunLength = maxRecords - numRecords;
              if (consumedRunLength > 0) {
                runLengthValues.add(currentValue, consumedRunLength);
                triplesRead += consumedRunLength;
                currentCount -= consumedRunLength;
              }
              return maxRecords;
            } else {
              numRecords += currentCount;
            }
          }
          runLengthValues.add(currentValue, currentCount);
          triplesRead += currentCount;
          currentCount = 0;
          break;

        case PACKED:
          int value = this.packedValuesBuffer[packedValuesBufferIdx];
          if (value == 0) {
            numRecords += 1;
            if (numRecords > maxRecords) {
              return maxRecords;
            }
          }
          runLengthValues.add(value, 1);
          currentCount--;
          packedValuesBufferIdx++;
          triplesRead++;
          break;

        default:
          throw new RuntimeException("Unrecognized mode: " + mode);
      }
    }
    return numRecords;
  }
}
