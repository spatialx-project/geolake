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

import java.util.Collections;
import java.util.Map;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
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

/** Vectorized reader for geometry values encoded as nested lists. */
public class VectorizedNestedListGeometryArrowReader implements VectorizedReader<VectorHolder> {

  private final Types.NestedField icebergField;

  private final ColumnDescriptor typeColumnDesc;
  private final ColumnDescriptor xColumnDesc;
  private final ColumnDescriptor yColumnDesc;
  private final ColumnDescriptor zColumnDesc;
  private final ColumnDescriptor mColumnDesc;
  private final ColumnDescriptor coordRangeColumnDesc;
  private final ColumnDescriptor lineRangeColumnDesc;
  private final ColumnDescriptor geomRangeColumnDesc;
  private final ColumnDescriptor geomTypesColumnDesc;

  private final VectorizedColumnIterator typeColumnIterator;
  private final VectorizedColumnIterator xColumnIterator;
  private final VectorizedColumnIterator yColumnIterator;
  private final VectorizedColumnIterator zColumnIterator;
  private final VectorizedColumnIterator mColumnIterator;
  private final VectorizedColumnIterator coordRangeColumnIterator;
  private final VectorizedColumnIterator lineRangeColumnIterator;
  private final VectorizedColumnIterator geomRangeColumnIterator;
  private final VectorizedColumnIterator geomTypesColumnIterator;

  private final BufferAllocator bufferAllocator;

  private StructVector vec;
  private IntVector typeVec;
  private ListVector xVec;
  private ListVector yVec;
  private ListVector zVec;
  private ListVector mVec;
  private ListVector coordRangeVec;
  private ListVector lineRangeVec;
  private ListVector geomRangeVec;
  private ListVector geomTypeVec;

  /**
   * Represents the nullability of the type column, which is also the nullability of the entire
   * geometry column
   */
  private NullabilityHolder nullabilityHolder;

  /**
   * Nullabilities of columns other than type were not represented by nullability holders, they were
   * represented by the validity buffers in each arrow field vector. This holder works as a scratch
   * space for accelerating vectorized reads of repeated fields.
   */
  private NullabilityHolder scratchNullabilityHolder;

  private boolean setArrowValidityVector;

  private int batchSize;

  public VectorizedNestedListGeometryArrowReader(
      MessageType type,
      String[] prefix,
      Types.NestedField icebergField,
      BufferAllocator bufferAllocator,
      boolean setArrowValidityVector) {
    this.icebergField = icebergField;
    this.setArrowValidityVector = setArrowValidityVector;

    typeColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "type"));
    xColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "x"));
    yColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "y"));
    zColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "z"));
    mColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "m"));
    coordRangeColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "coordinate_ranges"));
    lineRangeColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "line_ranges"));
    geomRangeColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "geometry_ranges"));
    geomTypesColumnDesc = type.getColumnDescription(ArrayUtil.add(prefix, "geometry_types"));

    // Build writers for columns for encoding geometry values. The validity of the entire struct is
    // recorded by the nullabilityHolder, while the validity of values in each individual column
    // were
    // recorded by the per-vector validity buffers.
    typeColumnIterator = new VectorizedColumnIterator(typeColumnDesc, "", true);
    xColumnIterator = new VectorizedColumnIterator(xColumnDesc, "", false);
    yColumnIterator = new VectorizedColumnIterator(yColumnDesc, "", false);
    zColumnIterator = new VectorizedColumnIterator(zColumnDesc, "", false);
    mColumnIterator = new VectorizedColumnIterator(mColumnDesc, "", false);
    coordRangeColumnIterator = new VectorizedColumnIterator(coordRangeColumnDesc, "", false);
    lineRangeColumnIterator = new VectorizedColumnIterator(lineRangeColumnDesc, "", false);
    geomRangeColumnIterator = new VectorizedColumnIterator(geomRangeColumnDesc, "", false);
    geomTypesColumnIterator = new VectorizedColumnIterator(geomTypesColumnDesc, "", false);

    this.bufferAllocator = bufferAllocator;
    this.batchSize = VectorizedArrowReader.DEFAULT_BATCH_SIZE;
  }

  @Override
  public VectorHolder read(VectorHolder reuse, int numRows) {
    if (vec == null || reuse == null) {
      allocateFieldVector();
      nullabilityHolder = new NullabilityHolder(batchSize);
      scratchNullabilityHolder = new NullabilityHolder(batchSize);
    } else {
      resetVecs();
      nullabilityHolder.reset();
      scratchNullabilityHolder.reset();
    }
    if (typeColumnIterator.hasNext()) {
      typeColumnIterator
          .integerBatchReader()
          .nextBatch(typeVec, IntVector.TYPE_WIDTH, nullabilityHolder);
      xColumnIterator
          .doubleRepeatedBatchReader()
          .nextBatch(xVec, Float8Vector.TYPE_WIDTH, scratchNullabilityHolder);
      yColumnIterator
          .doubleRepeatedBatchReader()
          .nextBatch(yVec, Float8Vector.TYPE_WIDTH, scratchNullabilityHolder);
      zColumnIterator
          .doubleRepeatedBatchReader()
          .nextBatch(zVec, Float8Vector.TYPE_WIDTH, scratchNullabilityHolder);
      mColumnIterator
          .doubleRepeatedBatchReader()
          .nextBatch(mVec, Float8Vector.TYPE_WIDTH, scratchNullabilityHolder);
      coordRangeColumnIterator
          .integerRepeatedBatchReader()
          .nextBatch(coordRangeVec, IntVector.TYPE_WIDTH, scratchNullabilityHolder);
      lineRangeColumnIterator
          .integerRepeatedBatchReader()
          .nextBatch(lineRangeVec, IntVector.TYPE_WIDTH, scratchNullabilityHolder);
      geomRangeColumnIterator
          .integerRepeatedBatchReader()
          .nextBatch(geomRangeVec, IntVector.TYPE_WIDTH, scratchNullabilityHolder);
      geomTypesColumnIterator
          .integerRepeatedBatchReader()
          .nextBatch(geomTypeVec, IntVector.TYPE_WIDTH, scratchNullabilityHolder);
    }
    verifyValueCount(typeVec, "type", numRows);
    verifyValueCount(xVec, "x", numRows);
    verifyValueCount(yVec, "y", numRows);
    verifyValueCount(zVec, "z", numRows);
    verifyValueCount(mVec, "m", numRows);
    verifyValueCount(coordRangeVec, "coordinate_ranges", numRows);
    verifyValueCount(lineRangeVec, "line_ranges", numRows);
    verifyValueCount(geomRangeVec, "geometry_ranges", numRows);
    verifyValueCount(geomTypeVec, "geometry_types", numRows);
    vec.setValueCount(typeVec.getValueCount());
    if (setArrowValidityVector) {
      setValidityBuffers();
    }

    return new GeometryVectorHolder(
        "nested-list", typeColumnDesc, vec, nullabilityHolder, icebergField.type());
  }

  @Override
  public void setBatchSize(int batchSize) {
    this.batchSize = (batchSize == 0) ? VectorizedArrowReader.DEFAULT_BATCH_SIZE : batchSize;
    typeColumnIterator.setBatchSize(this.batchSize);
    xColumnIterator.setBatchSize(this.batchSize);
    yColumnIterator.setBatchSize(this.batchSize);
    zColumnIterator.setBatchSize(this.batchSize);
    mColumnIterator.setBatchSize(this.batchSize);
    coordRangeColumnIterator.setBatchSize(this.batchSize);
    lineRangeColumnIterator.setBatchSize(this.batchSize);
    geomRangeColumnIterator.setBatchSize(this.batchSize);
    geomTypesColumnIterator.setBatchSize(this.batchSize);
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pages, Map<ColumnPath, ColumnChunkMetaData> metadata, long rowPosition) {
    typeColumnIterator.setRowGroupInfo(pages.getPageReader(typeColumnDesc), false);
    xColumnIterator.setRowGroupInfo(pages.getPageReader(xColumnDesc), false);
    yColumnIterator.setRowGroupInfo(pages.getPageReader(yColumnDesc), false);
    zColumnIterator.setRowGroupInfo(pages.getPageReader(zColumnDesc), false);
    mColumnIterator.setRowGroupInfo(pages.getPageReader(mColumnDesc), false);
    coordRangeColumnIterator.setRowGroupInfo(pages.getPageReader(coordRangeColumnDesc), false);
    lineRangeColumnIterator.setRowGroupInfo(pages.getPageReader(lineRangeColumnDesc), false);
    geomRangeColumnIterator.setRowGroupInfo(pages.getPageReader(geomRangeColumnDesc), false);
    geomTypesColumnIterator.setRowGroupInfo(pages.getPageReader(geomTypesColumnDesc), false);
  }

  @Override
  public void close() {
    if (vec != null) {
      vec.close();
    }
  }

  private void allocateFieldVector() {
    Field geometryField = buildFieldArrowSchema();
    vec = (StructVector) geometryField.createVector(bufferAllocator);
    typeVec = vec.getChild("type", IntVector.class);
    xVec = vec.getChild("x", ListVector.class);
    yVec = vec.getChild("y", ListVector.class);
    zVec = vec.getChild("z", ListVector.class);
    mVec = vec.getChild("m", ListVector.class);
    coordRangeVec = vec.getChild("coordinate_ranges", ListVector.class);
    lineRangeVec = vec.getChild("line_ranges", ListVector.class);
    geomRangeVec = vec.getChild("geometry_ranges", ListVector.class);
    geomTypeVec = vec.getChild("geometry_types", ListVector.class);
    vec.setInitialCapacity(batchSize);
    vec.allocateNewSafe();
  }

  private void resetVecs() {
    // We do not need to reset everything (offset buffers, data buffers, etc.). We've only assumed
    // that
    // the initial values of validity buffers of list vectors are 0, that's all we need to reset.
    vec.setValueCount(0);
    xVec.getValidityBuffer().setZero(0, xVec.getValidityBuffer().capacity());
    yVec.getValidityBuffer().setZero(0, yVec.getValidityBuffer().capacity());
    zVec.getValidityBuffer().setZero(0, zVec.getValidityBuffer().capacity());
    mVec.getValidityBuffer().setZero(0, mVec.getValidityBuffer().capacity());
    coordRangeVec.getValidityBuffer().setZero(0, coordRangeVec.getValidityBuffer().capacity());
    lineRangeVec.getValidityBuffer().setZero(0, lineRangeVec.getValidityBuffer().capacity());
    geomRangeVec.getValidityBuffer().setZero(0, geomRangeVec.getValidityBuffer().capacity());
    geomTypeVec.getValidityBuffer().setZero(0, geomTypeVec.getValidityBuffer().capacity());
  }

  private Field buildFieldArrowSchema() {
    Field typeField = new Field("type", FieldType.notNullable(new ArrowType.Int(32, true)), null);
    Field xField =
        new Field(
            "x",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field(
                    "element",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));
    Field yField =
        new Field(
            "y",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field(
                    "element",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));
    Field zField =
        new Field(
            "z",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field(
                    "element",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));
    Field mField =
        new Field(
            "m",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field(
                    "element",
                    FieldType.notNullable(
                        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                    null)));
    Field coordRangeField =
        new Field(
            "coordinate_ranges",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field("element", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
    Field lineRangeField =
        new Field(
            "line_ranges",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field("element", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
    Field geomRangeField =
        new Field(
            "geometry_ranges",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field("element", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
    Field geomTypeField =
        new Field(
            "geometry_types",
            FieldType.nullable(new ArrowType.List()),
            Collections.singletonList(
                new Field("element", FieldType.notNullable(new ArrowType.Int(32, true)), null)));
    ImmutableList<Field> children =
        ImmutableList.of(
            typeField,
            xField,
            yField,
            zField,
            mField,
            coordRangeField,
            lineRangeField,
            geomRangeField,
            geomTypeField);
    Field geometryField =
        new Field(
            icebergField.name(),
            new FieldType(icebergField.isOptional(), new ArrowType.Struct(), null, null),
            children);
    return geometryField;
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
