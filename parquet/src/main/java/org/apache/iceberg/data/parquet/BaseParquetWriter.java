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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.GeoParquetEnums.GeometryEncoding;
import org.apache.iceberg.parquet.GeoParquetEnums.GeometryValueType;
import org.apache.iceberg.parquet.GeoParquetUtil;
import org.apache.iceberg.parquet.GeoParquetValueWriters;
import org.apache.iceberg.parquet.ParquetTypeVisitor;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

public abstract class BaseParquetWriter<T> {

  @SuppressWarnings("unchecked")
  protected ParquetValueWriter<T> createWriter(MessageType type) {
    return (ParquetValueWriter<T>) ParquetTypeVisitor.visit(type, new WriteBuilder(type));
  }

  @SuppressWarnings("unchecked")
  protected ParquetValueWriter<T> createWriter(
      Schema tableSchema, MessageType fileSchema, Map<String, String> properties) {
    TableSchemaAwareWriteBuilder writeBuilder =
        new TableSchemaAwareWriteBuilder(fileSchema, properties);
    return (ParquetValueWriter<T>)
        TypeWithSchemaVisitor.visit(tableSchema.asStruct(), fileSchema, writeBuilder);
  }

  protected abstract ParquetValueWriters.StructWriter<T> createStructWriter(
      List<ParquetValueWriter<?>> writers);

  private class GenericWriteBuilder {
    private final MessageType type;
    private final Function<String, String[]> pathFunc;
    private final Supplier<String[]> currentPathSupplier;

    private GenericWriteBuilder(
        MessageType type,
        Function<String, String[]> pathFunc,
        Supplier<String[]> currentPathSupplier) {
      this.type = type;
      this.pathFunc = pathFunc;
      this.currentPathSupplier = currentPathSupplier;
    }

    private String[] path(String name) {
      return pathFunc.apply(name);
    }

    private String[] currentPath() {
      return currentPathSupplier.get();
    }

    public ParquetValueWriter<?> struct(
        GroupType struct, List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = struct.getType(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()));
        writers.add(ParquetValueWriters.option(fieldType, fieldD, fieldWriters.get(i)));
      }

      return createStructWriter(writers);
    }

    public ParquetValueWriter<?> list(GroupType array, ParquetValueWriter<?> elementWriter) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();
      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return ParquetValueWriters.collections(
          repeatedD, repeatedR, ParquetValueWriters.option(elementType, elementD, elementWriter));
    }

    public ParquetValueWriter<?> map(
        GroupType map, ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();
      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return ParquetValueWriters.maps(
          repeatedD,
          repeatedR,
          ParquetValueWriters.option(keyType, keyD, keyWriter),
          ParquetValueWriters.option(valueType, valueD, valueWriter));
    }

    public ParquetValueWriter<?> primitive(PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());
      LogicalTypeAnnotation logicalType = primitive.getLogicalTypeAnnotation();
      if (logicalType != null) {
        Optional<ParquetValueWriters.PrimitiveWriter<?>> writer =
            logicalType.accept(new LogicalTypeWriterVisitor(desc));
        if (writer.isPresent()) {
          return writer.get();
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedWriter(desc);
        case BINARY:
          return ParquetValueWriters.byteBuffers(desc);
        case BOOLEAN:
          return ParquetValueWriters.booleans(desc);
        case INT32:
          return ParquetValueWriters.ints(desc);
        case INT64:
          return ParquetValueWriters.longs(desc);
        case FLOAT:
          return ParquetValueWriters.floats(desc);
        case DOUBLE:
          return ParquetValueWriters.doubles(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private class TableSchemaAwareWriteBuilder extends TypeWithSchemaVisitor<ParquetValueWriter<?>> {
    private final GenericWriteBuilder writeBuilder;
    private final MessageType fileSchema;
    private final GeometryValueType geometryJavaType;

    private TableSchemaAwareWriteBuilder(MessageType fileSchema, Map<String, String> properties) {
      this.fileSchema = fileSchema;
      this.geometryJavaType =
          GeometryValueType.of(
              properties.getOrDefault(
                  "write.parquet.geometry.java-type", GeometryValueType.OBJECT.toString()));
      this.writeBuilder = new GenericWriteBuilder(fileSchema, this::path, this::currentPath);
    }

    @Override
    public ParquetValueWriter<?> message(
        Types.StructType tableFieldType,
        MessageType message,
        List<ParquetValueWriter<?>> fieldWriters) {
      return struct(tableFieldType, message.asGroupType(), fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(
        Types.StructType tableFieldType,
        GroupType struct,
        List<ParquetValueWriter<?>> fieldWriters) {
      return writeBuilder.struct(struct, fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(
        org.apache.iceberg.types.Type.PrimitiveType tableFieldType, GroupType struct) {
      if (tableFieldType != null && tableFieldType.typeId() == TypeID.GEOMETRY) {
        GeometryEncoding geometryEncoding = GeoParquetUtil.getGeometryEncodingOfGroupType(struct);
        switch (geometryEncoding) {
          case WKB_BBOX:
            return GeoParquetValueWriters.createGeometryWKBBBoxWriter(
                fileSchema, currentPath(), geometryJavaType);
          case NESTED_LIST:
            return GeoParquetValueWriters.createGeometryNestedListWriter(
                fileSchema, currentPath(), geometryJavaType);
          default:
            throw new UnsupportedOperationException(
                "Unsupported geometry encoding of group type " + struct);
        }
      } else {
        throw new UnsupportedOperationException(
            "Cannot create writer for writing " + tableFieldType + " as group type " + struct);
      }
    }

    @Override
    public ParquetValueWriter<?> list(
        Types.ListType tableFieldType, GroupType array, ParquetValueWriter<?> elementWriter) {
      return writeBuilder.list(array, elementWriter);
    }

    @Override
    public ParquetValueWriter<?> map(
        Types.MapType tableFieldType,
        GroupType map,
        ParquetValueWriter<?> keyWriter,
        ParquetValueWriter<?> valueWriter) {
      return writeBuilder.map(map, keyWriter, valueWriter);
    }

    @Override
    public ParquetValueWriter<?> primitive(
        org.apache.iceberg.types.Type.PrimitiveType tableFieldType, PrimitiveType primitive) {
      if (tableFieldType != null && tableFieldType.typeId() == TypeID.GEOMETRY) {
        ColumnDescriptor desc = fileSchema.getColumnDescription(currentPath());
        if (primitive.getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
          return GeoParquetValueWriters.createGeometryWKBWriter(desc, geometryJavaType);
        } else {
          throw new UnsupportedOperationException(
              "Encoding geometry values as " + primitive + " is not supported");
        }
      } else {
        return writeBuilder.primitive(primitive);
      }
    }
  }

  private class WriteBuilder extends ParquetTypeVisitor<ParquetValueWriter<?>> {
    private final GenericWriteBuilder writeBuilder;

    private WriteBuilder(MessageType type) {
      this.writeBuilder = new GenericWriteBuilder(type, this::path, this::currentPath);
    }

    @Override
    public ParquetValueWriter<?> message(
        MessageType message, List<ParquetValueWriter<?>> fieldWriters) {
      return struct(message.asGroupType(), fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(
        GroupType struct, List<ParquetValueWriter<?>> fieldWriters) {
      return writeBuilder.struct(struct, fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> list(GroupType array, ParquetValueWriter<?> elementWriter) {
      return writeBuilder.list(array, elementWriter);
    }

    @Override
    public ParquetValueWriter<?> map(
        GroupType map, ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
      return writeBuilder.map(map, keyWriter, valueWriter);
    }

    @Override
    public ParquetValueWriter<?> primitive(PrimitiveType primitive) {
      return writeBuilder.primitive(primitive);
    }
  }

  private static class LogicalTypeWriterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<
          ParquetValueWriters.PrimitiveWriter<?>> {
    private final ColumnDescriptor desc;

    private LogicalTypeWriterVisitor(ColumnDescriptor desc) {
      this.desc = desc;
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
      switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
        case INT32:
          return Optional.of(
              ParquetValueWriters.decimalAsInteger(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
        case INT64:
          return Optional.of(
              ParquetValueWriters.decimalAsLong(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return Optional.of(
              ParquetValueWriters.decimalAsFixed(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
      }
      return Optional.empty();
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.DateLogicalTypeAnnotation dateType) {
      return Optional.of(new DateWriter(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeType) {
      return Optional.of(new TimeWriter(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType) {
      Preconditions.checkArgument(
          LogicalTypeAnnotation.TimeUnit.MICROS.equals(timestampType.getUnit()),
          "Cannot write timestamp in %s, only MICROS is supported",
          timestampType.getUnit());
      if (timestampType.isAdjustedToUTC()) {
        return Optional.of(new TimestamptzWriter(desc));
      } else {
        return Optional.of(new TimestampWriter(desc));
      }
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
      Preconditions.checkArgument(
          intType.isSigned() || intType.getBitWidth() < 64,
          "Cannot read uint64: not a supported Java type");
      if (intType.getBitWidth() < 64) {
        return Optional.of(ParquetValueWriters.ints(desc));
      } else {
        return Optional.of(ParquetValueWriters.longs(desc));
      }
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonType) {
      return Optional.of(ParquetValueWriters.byteBuffers(desc));
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateWriter extends ParquetValueWriters.PrimitiveWriter<LocalDate> {
    private DateWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDate value) {
      column.writeInteger(repetitionLevel, (int) ChronoUnit.DAYS.between(EPOCH_DAY, value));
    }
  }

  private static class TimeWriter extends ParquetValueWriters.PrimitiveWriter<LocalTime> {
    private TimeWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalTime value) {
      column.writeLong(repetitionLevel, value.toNanoOfDay() / 1000);
    }
  }

  private static class TimestampWriter extends ParquetValueWriters.PrimitiveWriter<LocalDateTime> {
    private TimestampWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDateTime value) {
      column.writeLong(
          repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class TimestamptzWriter
      extends ParquetValueWriters.PrimitiveWriter<OffsetDateTime> {
    private TimestamptzWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, OffsetDateTime value) {
      column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value));
    }
  }

  private static class FixedWriter extends ParquetValueWriters.PrimitiveWriter<byte[]> {
    private FixedWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, byte[] value) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(value));
    }
  }
}
