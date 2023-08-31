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
package org.apache.iceberg.data.parquet;

import java.util.Map;
import org.apache.iceberg.parquet.GeoParquetEnums;
import org.apache.iceberg.parquet.GeoParquetUtil;
import org.apache.iceberg.parquet.GeoParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

public class GeoParquetReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
  protected final MessageType type;
  protected final Map<Integer, ?> idToConstant;
  protected final GeoParquetEnums.GeometryValueType geometryJavaType;

  public GeoParquetReadBuilder(
      MessageType type, Map<Integer, ?> idToConstant, Map<String, String> properties) {
    this.type = type;
    this.idToConstant = idToConstant;
    this.geometryJavaType =
        GeoParquetEnums.GeometryValueType.of(
            properties.getOrDefault(
                "read.parquet.geometry.java-type",
                GeoParquetEnums.GeometryValueType.JTS_GEOMETRY.toString()));
  }

  @Override
  public ParquetValueReader<?> struct(
      org.apache.iceberg.types.Type.PrimitiveType iPrimitive, GroupType struct) {
    if (iPrimitive != null
        && iPrimitive.typeId() == org.apache.iceberg.types.Type.TypeID.GEOMETRY) {
      GeoParquetEnums.GeometryEncoding geometryEncoding =
          GeoParquetUtil.getGeometryEncodingOfGroupType(struct);
      switch (geometryEncoding) {
        case WKB_BBOX:
          return GeoParquetValueReaders.createGeometryWKBBBoxReader(
              type, currentPath(), geometryJavaType);
        case NESTED_LIST:
          return GeoParquetValueReaders.createGeometryNestedListReader(
              type, currentPath(), geometryJavaType);
        default:
          throw new UnsupportedOperationException(
              "Unsupported geometry encoding of group type " + struct);
      }
    } else {
      throw new UnsupportedOperationException(
          "Cannot create reader for reading group type "
              + struct
              + " as primitive type "
              + iPrimitive);
    }
  }
}
