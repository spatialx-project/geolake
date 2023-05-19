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
package org.apache.iceberg.arrow.vectorized;

import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.arrow.vectorized.VectorHolder.GeometryVectorHolder;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/** Vectorized reader for geometry values encoded as plain WKBs. */
public class VectorizedWKBArrowReader implements VectorizedReader<VectorHolder> {

  private static final int AVERAGE_VARIABLE_WIDTH_RECORD_SIZE = 10;

  private final Types.NestedField icebergField;
  private final ColumnDescriptor columnDescriptor;
  private final VectorizedColumnIterator columnIterator;

  private final BufferAllocator bufferAllocator;
  private VarBinaryVector vec;
  private NullabilityHolder nullabilityHolder;

  private int batchSize;

  public VectorizedWKBArrowReader(
      ColumnDescriptor desc,
      Types.NestedField icebergField,
      BufferAllocator bufferAllocator,
      boolean setArrowValidityVector) {
    this.icebergField = icebergField;
    this.columnDescriptor = desc;
    this.columnIterator =
        new VectorizedColumnIterator(columnDescriptor, "", setArrowValidityVector);
    this.bufferAllocator = bufferAllocator;
    this.batchSize = VectorizedArrowReader.DEFAULT_BATCH_SIZE;
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numRows) {
    if (vec == null || reuse == null) {
      allocateFieldVector();
      nullabilityHolder = new NullabilityHolder(batchSize);
    } else {
      vec.setValueCount(0);
      nullabilityHolder.reset();
    }
    if (columnIterator.hasNext()) {
      columnIterator.varWidthTypeBatchReader().nextBatch(vec, -1, nullabilityHolder);
    }
    Preconditions.checkState(
        vec.getValueCount() == numRows,
        "Number of %s values read, %d, does not equal expected, %d",
        icebergField.name(),
        vec.getValueCount(),
        numRows);
    return new GeometryVectorHolder("wkb", columnDescriptor, vec, nullabilityHolder, icebergField);
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? VectorizedArrowReader.DEFAULT_BATCH_SIZE : batchSize;
    columnIterator.setBatchSize(this.batchSize);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata, long rowPosition) {
    columnIterator.setRowGroupInfo(pages.getPageReader(columnDescriptor), false);
  }

  @Override
  public void close() {
    if (vec != null) {
      vec.close();
    }
  }

  private void allocateFieldVector() {
    Field field =
        new Field(icebergField.name(), FieldType.notNullable(new ArrowType.Binary()), null);
    vec = (VarBinaryVector) field.createVector(bufferAllocator);
    vec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
    vec.allocateNewSafe();
  }
}
