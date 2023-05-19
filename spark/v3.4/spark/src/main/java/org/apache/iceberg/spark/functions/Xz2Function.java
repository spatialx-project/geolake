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
package org.apache.iceberg.spark.functions;

import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.transforms.geometry.XZ2SFCurving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.iceberg.udt.GeometrySerializer;
import org.apache.spark.sql.iceberg.udt.GeometryUDT;
import org.apache.spark.sql.iceberg.udt.GeometryUDT$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/**
 * A Spark function implementation for the Iceberg xz2 transform.
 *
 * <p>Example usage: {@code SELECT system.xz2('source_col', 10)}.
 *
 * <p>Note that for performance reasons, the given input resolution is not validated in the
 * implementations used in code-gen. The width must remain non-negative to give meaningful results.
 */
public class Xz2Function implements UnboundFunction {
  private static final int WIDTH_ORDINAL = 1;
  private static final int VALUE_ORDINAL = 0;

  private static final Set<DataType> SUPPORTED_WIDTH_TYPES =
      ImmutableSet.of(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType);

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.size() != 2) {
      throw new UnsupportedOperationException("Wrong number of inputs (expected width and value)");
    }

    StructField widthField = inputType.fields()[WIDTH_ORDINAL];
    StructField valueField = inputType.fields()[VALUE_ORDINAL];

    if (!SUPPORTED_WIDTH_TYPES.contains(widthField.dataType())) {
      throw new UnsupportedOperationException("Expected xz2 width to be tinyint, shortint or int");
    }
    DataType valueType = valueField.dataType();
    if (valueType instanceof GeometryUDT) {
      return new GeometryToXzFunction();
    } else {
      throw new UnsupportedOperationException(
          "Expected value to be GeometryUDT: " + valueType.catalogString());
    }
  }

  @Override
  public String description() {
    return name()
        + "(resolution, col) - Call Iceberg's xz2 transform\n"
        + "  resolution :: resolution for xz2, e.g. xz2(10, IcebergSTGeomFromText('Point(1 2)')) (must be an Long)\n"
        + "  col :: column to truncate (must be a geometry)";
  }

  @Override
  public String name() {
    return "xz2";
  }

  public static class GeometryToXzFunction implements ScalarFunction<Long> {
    // magic method used in codegen
    public static Long invoke(int width, byte[] bytes) {
      XZ2SFCurving sfc = new XZ2SFCurving(width);
      Geometry geom = GeometrySerializer.deserialize(bytes);
      Envelope envelope = geom.getEnvelopeInternal();
      return sfc.index(
          envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
    }

    @Override
    public String name() {
      return "xz2";
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {GeometryUDT$.MODULE$, DataTypes.IntegerType};
    }

    @Override
    public DataType resultType() {
      return DataTypes.LongType;
    }

    @Override
    public String canonicalName() {
      return "iceberg.xz2(geometry)";
    }

    @Override
    public Long produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      if (input.isNullAt(WIDTH_ORDINAL) || input.isNullAt(VALUE_ORDINAL)) {
        return null;
      } else {
        return invoke(input.getInt(WIDTH_ORDINAL), input.getBinary(VALUE_ORDINAL));
      }
    }
  }
}
