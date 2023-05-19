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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.iceberg.arrow.vectorized.VectorHolder.GeometryVectorHolder;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;

/** Vectorized reader for geometry values encoded as groups containing WKBs and bounding boxes. */
public class VectorizedWKBBBoxArrowReader implements VectorizedReader<VectorHolder> {

  private static final int AVERAGE_VARIABLE_WIDTH_RECORD_SIZE = 10;

  private final Types.NestedField icebergField;

  private final ColumnDescriptor wkbColumnDesc;
  private final ColumnDescriptor minXColumnDesc;
  private final ColumnDescriptor minYColumnDesc;
  private final ColumnDescriptor maxXColumnDesc;
  private final ColumnDescriptor maxYColumnDesc;

  private final VectorizedColumnIterator wkbColumnIterator;
  private final VectorizedColumnIterator minXColumnIterator;
  private final VectorizedColumnIterator minYColumnIterator;
  private final VectorizedColumnIterator maxXColumnIterator;
  private final VectorizedColumnIterator maxYColumnIterator;

  private final BufferAllocator bufferAllocator;
  private StructVector vec;
  private VarBinaryVector wkbVec;
  private Float8Vector minXVec;
  private Float8Vector minYVec;
  private Float8Vector maxXVec;
  private Float8Vector maxYVec;

  /**
   * Represents the nullability of the wkb column, which is also the nullability of the entire
   * geometry column
   */
  private NullabilityHolder nullabilityHolder;

  /**
   * Nullabilities of bbox columns were not represented by nullability holders, they were
   * represented by the validity buffers in each arrow field vector.
   */
  private NullabilityHolder dummyNullabilityHolder;

  private final boolean setArrowValidityVector;

  private int batchSize;

  public VectorizedWKBBBoxArrowReader(
      MessageType type,
      String[] prefix,
      Types.NestedField icebergField,
      BufferAllocator bufferAllocator,
      boolean setArrowValidityVector) {
    this.icebergField = icebergField;
    this.setArrowValidityVector = setArrowValidityVector;

    wkbColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "wkb"));
    minXColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_x"));
    minYColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_y"));
    maxXColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_x"));
    maxYColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_y"));

    // Build column writers for wkb column and bbox columns. The validity of the entire struct is
    // recorded by
    // nullabilityHolder, while the validity of bbox values were recorded by the per-vector validity
    // buffers.
    wkbColumnIterator = new VectorizedColumnIterator(wkbColumnDesc, "", true);
    minXColumnIterator = new VectorizedColumnIterator(minXColumnDesc, "", true);
    minYColumnIterator = new VectorizedColumnIterator(minYColumnDesc, "", true);
    maxXColumnIterator = new VectorizedColumnIterator(maxXColumnDesc, "", true);
    maxYColumnIterator = new VectorizedColumnIterator(maxYColumnDesc, "", true);
    this.bufferAllocator = bufferAllocator;
    this.batchSize = VectorizedArrowReader.DEFAULT_BATCH_SIZE;
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numRows) {
    if (vec == null || reuse == null) {
      allocateFieldVector();
      nullabilityHolder = new NullabilityHolder(batchSize);
      dummyNullabilityHolder = new NullabilityHolder(batchSize);
    } else {
      vec.setValueCount(0);
      nullabilityHolder.reset();
      dummyNullabilityHolder.reset();
    }
    if (wkbColumnIterator.hasNext()) {
      int doubleTypeWidth = Float8Vector.TYPE_WIDTH;
      wkbColumnIterator.varWidthTypeBatchReader().nextBatch(wkbVec, -1, nullabilityHolder);
      minXColumnIterator
          .doubleBatchReader()
          .nextBatch(minXVec, doubleTypeWidth, dummyNullabilityHolder);
      minYColumnIterator
          .doubleBatchReader()
          .nextBatch(minYVec, doubleTypeWidth, dummyNullabilityHolder);
      maxXColumnIterator
          .doubleBatchReader()
          .nextBatch(maxXVec, doubleTypeWidth, dummyNullabilityHolder);
      maxYColumnIterator
          .doubleBatchReader()
          .nextBatch(maxYVec, doubleTypeWidth, dummyNullabilityHolder);
    }
    verifyValueCount(wkbVec, "wkb", numRows);
    verifyValueCount(minXVec, "min_x", numRows);
    verifyValueCount(minYVec, "min_y", numRows);
    verifyValueCount(maxXVec, "max_x", numRows);
    verifyValueCount(maxYVec, "max_y", numRows);
    vec.setValueCount(wkbVec.getValueCount());
    if (setArrowValidityVector) {
      setValidityBuffers();
    }

    return new GeometryVectorHolder(
        "wkb-bbox", wkbColumnDesc, vec, nullabilityHolder, icebergField);
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? VectorizedArrowReader.DEFAULT_BATCH_SIZE : batchSize;
    wkbColumnIterator.setBatchSize(this.batchSize);
    minXColumnIterator.setBatchSize(this.batchSize);
    minYColumnIterator.setBatchSize(this.batchSize);
    maxXColumnIterator.setBatchSize(this.batchSize);
    maxYColumnIterator.setBatchSize(this.batchSize);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata, long rowPosition) {
    wkbColumnIterator.setRowGroupInfo(pages.getPageReader(wkbColumnDesc), false);
    minXColumnIterator.setRowGroupInfo(pages.getPageReader(minXColumnDesc), false);
    minYColumnIterator.setRowGroupInfo(pages.getPageReader(minYColumnDesc), false);
    maxXColumnIterator.setRowGroupInfo(pages.getPageReader(maxXColumnDesc), false);
    maxYColumnIterator.setRowGroupInfo(pages.getPageReader(maxYColumnDesc), false);
  }

  @Override
  public void close() {
    if (vec != null) {
      vec.close();
    }
  }

  private void allocateFieldVector() {
    Field wkbField = new Field("wkb", FieldType.notNullable(new ArrowType.Binary()), null);
    Field minXField =
        new Field(
            "min_x",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field minYField =
        new Field(
            "min_y",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field maxXField =
        new Field(
            "max_x",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    Field maxYField =
        new Field(
            "max_y",
            FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
            null);
    ImmutableList<Field> children =
        ImmutableList.of(wkbField, minXField, minYField, maxXField, maxYField);
    Field geometryField =
        new Field(
            icebergField.name(),
            new FieldType(icebergField.isOptional(), new ArrowType.Struct(), null, null),
            children);
    vec = (StructVector) geometryField.createVector(bufferAllocator);
    wkbVec = vec.getChild("wkb", VarBinaryVector.class);
    minXVec = vec.getChild("min_x", Float8Vector.class);
    minYVec = vec.getChild("min_y", Float8Vector.class);
    maxXVec = vec.getChild("max_x", Float8Vector.class);
    maxYVec = vec.getChild("max_y", Float8Vector.class);
    vec.setInitialCapacity(batchSize);
    wkbVec.setInitialCapacity(batchSize * AVERAGE_VARIABLE_WIDTH_RECORD_SIZE);
    vec.allocateNewSafe();
  }

  private void setValidityBuffers() {
    ArrowBuf structValidityBuffer = vec.getValidityBuffer();
    for (int k = 0; k < nullabilityHolder.size(); k++) {
      int isValid = (nullabilityHolder.isNullAt(k) == 0 ? 1 : 0);
      BitVectorHelper.setValidityBit(structValidityBuffer, k, isValid);
    }
  }

  private void verifyValueCount(FieldVector vector, String name, int numRows) {
    Preconditions.checkState(
        vector.getValueCount() == numRows,
        "Number of %s values read, %d, does not equal expected, %d",
        name,
        vector.getValueCount(),
        numRows);
  }
}
