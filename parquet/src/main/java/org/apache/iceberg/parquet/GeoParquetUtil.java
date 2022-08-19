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

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.MetricsModes.MetricsMode;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.GeoParquetEnums.GeometryEncoding;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;

/** Utility for generating metadata for geoparquet files */
public class GeoParquetUtil {

  private GeoParquetUtil() {}

  private static final String VERSION = "0.4.0";

  public static final List<org.apache.parquet.schema.Type> NESTED_LIST_GEOMETRY_FIELDS =
      ImmutableList.of(
          org.apache.parquet.schema.Types.required(INT32).named("type"),
          org.apache.parquet.schema.Types.repeated(DOUBLE).named("x"),
          org.apache.parquet.schema.Types.repeated(DOUBLE).named("y"),
          org.apache.parquet.schema.Types.repeated(DOUBLE).named("z"),
          org.apache.parquet.schema.Types.repeated(DOUBLE).named("m"),
          org.apache.parquet.schema.Types.repeated(INT32).named("coordinate_ranges"),
          org.apache.parquet.schema.Types.repeated(INT32).named("line_ranges"),
          org.apache.parquet.schema.Types.repeated(INT32).named("geometry_ranges"),
          org.apache.parquet.schema.Types.repeated(INT32).named("geometry_types"));

  public static final List<org.apache.parquet.schema.Type> WKB_WITH_BBOX_GEOMETRY_FIELDS =
      ImmutableList.of(
          org.apache.parquet.schema.Types.required(BINARY).named("wkb"),
          org.apache.parquet.schema.Types.optional(DOUBLE).named("min_x"),
          org.apache.parquet.schema.Types.optional(DOUBLE).named("min_y"),
          org.apache.parquet.schema.Types.optional(DOUBLE).named("max_x"),
          org.apache.parquet.schema.Types.optional(DOUBLE).named("max_y"));

  /**
   * Get parquet field type for geometry field in iceberg schema
   *
   * @param repetition repetition level of parquet field
   * @param id id of iceberg geometry field
   * @param name name of iceberg geometry field
   * @param geometryEncoding encoding of geometry values in GeoParquet file
   * @return Type of geometry field
   */
  public static org.apache.parquet.schema.Type getTypeForGeometryField(
      org.apache.parquet.schema.Type.Repetition repetition,
      int id,
      String name,
      GeometryEncoding geometryEncoding) {
    switch (geometryEncoding) {
      case WKB:
        return org.apache.parquet.schema.Types.primitive(BINARY, repetition).id(id).named(name);
      case WKB_BBOX:
        return org.apache.parquet.schema.Types.buildGroup(repetition)
            .addFields(
                Iterables.toArray(
                    WKB_WITH_BBOX_GEOMETRY_FIELDS, org.apache.parquet.schema.Type.class))
            .id(id)
            .named(name);
      case NESTED_LIST:
        return org.apache.parquet.schema.Types.buildGroup(repetition)
            .addFields(
                Iterables.toArray(
                    NESTED_LIST_GEOMETRY_FIELDS, org.apache.parquet.schema.Type.class))
            .id(id)
            .named(name);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geoparquet geometry encoding: " + geometryEncoding);
    }
  }

  /**
   * Get geometry encoding of geometry column given group type of geometry field
   *
   * @param groupType group type of geometry field
   * @return geometry encoding
   */
  public static GeometryEncoding getGeometryEncodingOfGroupType(GroupType groupType) {
    if (NESTED_LIST_GEOMETRY_FIELDS.equals(groupType.getFields())) {
      return GeometryEncoding.NESTED_LIST;
    } else if (WKB_WITH_BBOX_GEOMETRY_FIELDS.equals(groupType.getFields())) {
      return GeometryEncoding.WKB_BBOX;
    } else {
      throw new IllegalArgumentException("Unknown geometry encoding of group type " + groupType);
    }
  }

  /**
   * Test if group type represents a geometry field in iceberg schema.
   *
   * @param groupType group type
   * @return if group type represents a geometry field
   */
  public static boolean isGroupTypeRepresentsGeometry(GroupType groupType) {
    try {
      getGeometryEncodingOfGroupType(groupType);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Get ID of geometry field when column is inside a geometry field persisted as nested lists.
   *
   * @param column column chunk metadata
   * @param parquetTypeWithIds parquet file schema with field IDs
   * @param fileSchema iceberg table schema
   * @return Field ID of geometry field, or return null if column is not inside a geometry field.
   */
  public static Integer getFieldIdOfNestedListGeometry(
      ColumnChunkMetaData column, MessageType parquetTypeWithIds, Schema fileSchema) {
    String[] columnPath = column.getPath().toArray();
    if (columnPath.length <= 1) {
      return null;
    }
    String[] parentPath = Arrays.copyOfRange(columnPath, 0, columnPath.length - 1);
    org.apache.parquet.schema.Type parentType = parquetTypeWithIds.getType(parentPath);
    if (parentType.getId() == null) {
      return null;
    }
    int fieldId = parentType.getId().intValue();
    NestedField tableField = fileSchema.findField(fieldId);
    if (tableField == null || tableField.type().typeId() != TypeID.GEOMETRY) {
      return null;
    }
    return fieldId;
  }

  /**
   * Update geometry statistics when geometry was stored as nested lists.
   *
   * @param column column chunk metadata
   * @param fileSchema iceberg table schema
   * @param fieldId ID of geometry field
   * @param metricsConfig metrics config
   * @param columnSizes (output parameter) size of columns
   * @param valueCounts (output parameter) value counts of columns
   * @param nullValueCounts (output parameter) null values counts of columns
   * @param missingStats (output parameter) fields without column stats
   */
  public static void updateStatisticsForNestedListGeometry(
      ColumnChunkMetaData column,
      Schema fileSchema,
      int fieldId,
      MetricsConfig metricsConfig,
      Map<Integer, Long> columnSizes,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Set<Integer> missingStats) {
    // column is inside some geometry field, accumulate column size
    columnSizes.put(fieldId, column.getTotalSize() + columnSizes.getOrDefault(fieldId, 0L));

    // don't accumulate statistics other than column size if metrics for this field was disabled.
    MetricsMode metricsMode = MetricsUtil.metricsMode(fileSchema, metricsConfig, fieldId);
    if (metricsMode == MetricsModes.None.get()) {
      return;
    }

    // we'll only accumulate value counts for a particular field, which avoids accumulating value
    // counts
    // repeatedly for columns in geometry group.
    String[] columnPath = column.getPath().toArray();
    String innerColumnName = columnPath[columnPath.length - 1];
    if (
    /* NESTED_LIST */ "type".equals(innerColumnName)
        /* WKB_BBOX */ || "wkb".equals(innerColumnName)) {
      valueCounts.put(fieldId, column.getValueCount() + valueCounts.getOrDefault(fieldId, 0L));
      Statistics<?> stats = column.getStatistics();
      if (stats == null) {
        missingStats.add(fieldId);
      } else if (!stats.isEmpty()) {
        nullValueCounts.put(
            fieldId, stats.getNumNulls() + nullValueCounts.getOrDefault(fieldId, 0L));
      }
    }
  }

  /**
   * Assemble metadata for GeoParquet file
   *
   * @param tableSchema Schema of iceberg table
   * @param geometryEncoding Encoding of geometry values in parquet files
   * @param metrics Per column metrics
   * @return GeoParquet metadata, null if there's no geometry column in table
   * @throws IOException when writing GeoParquet metadata failed
   */
  public static String assembleGeoParquetMetadata(
      Schema tableSchema, GeometryEncoding geometryEncoding, Stream<FieldMetrics<?>> metrics)
      throws IOException {
    Map<Integer, FieldMetrics<?>> metricsMap =
        metrics.collect(Collectors.toMap(FieldMetrics::id, Function.identity()));
    String encoding = getEncoding(geometryEncoding);
    try (StringWriter writer = new StringWriter();
        JsonGenerator generator = JsonUtil.factory().createGenerator(writer)) {
      generator.writeStartObject();
      generator.writeStringField("version", VERSION);
      generator.writeObjectFieldStart("columns");
      String primaryGeometryColumnName = "";

      List<Types.NestedField> fields = tableSchema.asStruct().fields();
      for (Types.NestedField field : fields) {
        if (field.type().typeId() == Type.TypeID.GEOMETRY) {
          // add geometry column info and bbox to parquet metadata
          if (primaryGeometryColumnName.isEmpty()) {
            primaryGeometryColumnName = field.name();
          }
          FieldMetrics<?> fieldMetrics = metricsMap.get(field.fieldId());
          writeGeometryMetadata(field, encoding, fieldMetrics, generator);
        }
      }

      if (!primaryGeometryColumnName.isEmpty()) {
        generator.writeEndObject();
        generator.writeStringField("primary_column", primaryGeometryColumnName);
        generator.writeEndObject();
        generator.flush();
        return writer.toString();
      } else {
        return null;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void writeGeometryMetadata(
      Types.NestedField field, String encoding, FieldMetrics<?> metrics, JsonGenerator generator)
      throws IOException {
    generator.writeObjectFieldStart(field.name());
    generator.writeStringField("encoding", encoding);
    generator.writeStringField("geometry_type", "Unknown");
    generator.writeObjectFieldStart("crs");
    generator.writeObjectFieldStart("id");
    generator.writeStringField("authority", "OGC");
    generator.writeStringField("code", "CRS84");
    generator.writeEndObject();
    generator.writeEndObject();
    generator.writeStringField("edges", "planar");
    if (metrics != null) {
      Pair<Double, Double> lowerBound = (Pair<Double, Double>) metrics.lowerBound();
      Pair<Double, Double> upperBound = (Pair<Double, Double>) metrics.upperBound();
      if (lowerBound != null && upperBound != null) {
        generator.writeArrayFieldStart("bbox");
        generator.writeNumber(lowerBound.first());
        generator.writeNumber(lowerBound.second());
        generator.writeNumber(upperBound.first());
        generator.writeNumber(upperBound.second());
        generator.writeEndArray();
      }
    }
    generator.writeEndObject();
  }

  private static String getEncoding(GeometryEncoding geometryEncoding) {
    switch (geometryEncoding) {
      case WKB:
        return "WKB";
      case WKB_BBOX:
        return "WKB_BBOX";
      case NESTED_LIST:
        return "NESTED_LIST";
      default:
        throw new UnsupportedOperationException(
            "Unsupported geoparquet geometry encoding: " + geometryEncoding);
    }
  }
}
