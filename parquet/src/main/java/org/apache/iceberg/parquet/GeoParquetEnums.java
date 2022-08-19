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
package org.apache.iceberg.parquet;

public class GeoParquetEnums {

  /** Geometry encoding defines how geometry values were persisted in GeoParquet files. */
  public enum GeometryEncoding {
    /** Encode geometry as WKB */
    WKB("wkb"),

    /** Encode geometry as a group containing WKB and bounding box */
    WKB_BBOX("wkb-bbox"),

    /** Encode geometry as a group containing coordinates and nested list indices */
    NESTED_LIST("nested-list");

    private final String value;

    GeometryEncoding(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static GeometryEncoding of(String value) {
      for (GeometryEncoding encoding : GeometryEncoding.values()) {
        if (encoding.value.equals(value)) {
          return encoding;
        }
      }
      throw new IllegalArgumentException("Unrecognized geometry encoding " + value);
    }
  }

  /**
   * Geometry value type defines how to interact with data file readers and writers, such as which
   * type of value should we pass to geometry value writers, and which type of value should we
   * expect to retrieve from geometry value readers.
   */
  public enum GeometryValueType {
    /** Represents geometry value as generic object, only used by geometry value writers */
    OBJECT("object"),

    /** Read/write geometry value as ByteBuffer, which contains WKB serialized geometry */
    BYTE_BUFFER("byte-buffer"),

    /** Read/write geometry value as JTS Geometry object */
    JTS_GEOMETRY("jts-geometry");

    private final String value;

    GeometryValueType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static GeometryValueType of(String value) {
      for (GeometryValueType valueType : GeometryValueType.values()) {
        if (valueType.value.equals(value)) {
          return valueType;
        }
      }
      throw new IllegalArgumentException("Unrecognized geometry value type " + value);
    }
  }
}
