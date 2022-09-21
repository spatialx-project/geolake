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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.GeometryFieldMetrics;
import org.apache.iceberg.parquet.GeoParquetEnums.GeometryValueType;
import org.apache.iceberg.parquet.ParquetValueWriters.PrimitiveWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil.GeometryUtils;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBConstants;

/**
 * Geometry value writers for GeoParquet, which is a variant of parquet for storing geospatial data.
 */
public class GeoParquetValueWriters {

  private GeoParquetValueWriters() {}

  public static ParquetValueWriter<?> createGeometryWKBWriter(
      ColumnDescriptor desc, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case OBJECT:
        return new GeometryGenericWKBWriter<>(desc);
      case JTS_GEOMETRY:
        return new GeometryJtsWKBWriter(desc);
      case BYTE_BUFFER:
        return new GeometryByteBufferWKBWriter(desc);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  public static ParquetValueWriter<?> createGeometryNestedListWriter(
      MessageType type, String[] prefix, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case OBJECT:
        return new GeometryGenericNestedListWriter<>(type, prefix);
      case JTS_GEOMETRY:
        return new GeometryJtsNestedListWriter(type, prefix);
      case BYTE_BUFFER:
        return new GeometryByteBufferNestedListWriter(type, prefix);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  public static ParquetValueWriter<?> createGeometryWKBBBoxWriter(
      MessageType type, String[] prefix, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case OBJECT:
        return new GeometryGenericWKBBBoxWriter<>(type, prefix);
      case JTS_GEOMETRY:
        return new GeometryJtsWKBBBoxWriter(type, prefix);
      case BYTE_BUFFER:
        return new GeometryByteBufferWKBBBoxWriter(type, prefix);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  public static class GeometryGenericWKBWriter<T> extends PrimitiveWriter<T> {
    private final GeometryFieldMetrics.GenericBuilder<T> metricsBuilder;

    public GeometryGenericWKBWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder<>(id);
    }

    @Override
    public void write(int repetitionLevel, T obj) {
      if (obj instanceof ByteBuffer) {
        writeByteBuffer(repetitionLevel, (ByteBuffer) obj);
      } else if (obj instanceof Geometry) {
        writeJtsGeometry(repetitionLevel, (Geometry) obj);
      } else {
        throw new IllegalArgumentException(
            "Cannot write object of type " + obj.getClass() + " as geometry value");
      }
    }

    protected void writeByteBuffer(int repetitionLevel, ByteBuffer byteBuffer) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(byteBuffer));
      metricsBuilder.addByteBuffer(byteBuffer);
    }

    protected void writeJtsGeometry(int repetitionLevel, Geometry geom) {
      ByteBuffer buffer = GeometryUtils.geometry2byteBuffer(geom);
      column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(buffer));
      metricsBuilder.addJtsGeometry(geom);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static class GeometryByteBufferWKBWriter extends GeometryGenericWKBWriter<ByteBuffer> {
    GeometryByteBufferWKBWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, ByteBuffer buffer) {
      writeByteBuffer(repetitionLevel, buffer);
    }
  }

  private static class GeometryJtsWKBWriter extends GeometryGenericWKBWriter<Geometry> {
    GeometryJtsWKBWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Geometry geom) {
      writeJtsGeometry(repetitionLevel, geom);
    }
  }

  public static class GeometryGenericWKBBBoxWriter<T> implements ParquetValueWriter<T> {
    private final int definitionLevel;

    private final ColumnWriter<Binary> wkbColumnWriter;
    private final ColumnWriter<Double> minXColumnWriter;
    private final ColumnWriter<Double> minYColumnWriter;
    private final ColumnWriter<Double> maxXColumnWriter;
    private final ColumnWriter<Double> maxYColumnWriter;
    private final List<TripleWriter<?>> children;
    private final GeometryFieldMetrics.GenericBuilder<T> metricsBuilder;

    public GeometryGenericWKBBBoxWriter(MessageType type, String[] prefix) {
      ColumnDescriptor wkbDesc = type.getColumnDescription(ArrayUtil.add(prefix, "wkb"));
      ColumnDescriptor minXDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_x"));
      ColumnDescriptor minYDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_y"));
      ColumnDescriptor maxXDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_x"));
      ColumnDescriptor maxYDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_y"));
      definitionLevel = minXDesc.getMaxDefinitionLevel();

      wkbColumnWriter = ColumnWriter.newWriter(wkbDesc);
      minXColumnWriter = ColumnWriter.newWriter(minXDesc);
      minYColumnWriter = ColumnWriter.newWriter(minYDesc);
      maxXColumnWriter = ColumnWriter.newWriter(maxXDesc);
      maxYColumnWriter = ColumnWriter.newWriter(maxYDesc);
      children =
          ImmutableList.of(
              wkbColumnWriter,
              minXColumnWriter,
              minYColumnWriter,
              maxXColumnWriter,
              maxYColumnWriter);

      int id = type.getType(prefix).getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder<>(id);
    }

    @Override
    public void write(int parentRepetition, T obj) {
      if (obj instanceof ByteBuffer) {
        writeByteBuffer(parentRepetition, (ByteBuffer) obj);
      } else if (obj instanceof Geometry) {
        writeJtsGeometry(parentRepetition, (Geometry) obj);
      } else {
        throw new IllegalArgumentException(
            "Cannot write object of type " + obj.getClass() + " as geometry value");
      }
    }

    protected void writeByteBuffer(int parentRepetition, ByteBuffer byteBuffer) {
      wkbColumnWriter.writeBinary(parentRepetition, Binary.fromReusedByteBuffer(byteBuffer));
      Geometry geometry = GeometryUtils.byteBuffer2geometry(byteBuffer);
      Envelope env = geometry.getEnvelopeInternal();
      writeEnvelope(parentRepetition, env);
      metricsBuilder.addEnvelope(env);
    }

    protected void writeJtsGeometry(int parentRepetition, Geometry geom) {
      ByteBuffer wkb = GeometryUtils.geometry2byteBuffer(geom);
      wkbColumnWriter.writeBinary(parentRepetition, Binary.fromReusedByteBuffer(wkb));
      Envelope env = geom.getEnvelopeInternal();
      writeEnvelope(parentRepetition, env);
      metricsBuilder.addEnvelope(env);
    }

    private void writeEnvelope(int parentRepetition, Envelope env) {
      if (!env.isNull()) {
        minXColumnWriter.writeDouble(parentRepetition, env.getMinX());
        minYColumnWriter.writeDouble(parentRepetition, env.getMinY());
        maxXColumnWriter.writeDouble(parentRepetition, env.getMaxX());
        maxYColumnWriter.writeDouble(parentRepetition, env.getMaxY());
      } else {
        minXColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        minYColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        maxXColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        maxYColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      wkbColumnWriter.setColumnStore(columnStore);
      minXColumnWriter.setColumnStore(columnStore);
      minYColumnWriter.setColumnStore(columnStore);
      maxXColumnWriter.setColumnStore(columnStore);
      maxYColumnWriter.setColumnStore(columnStore);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static class GeometryByteBufferWKBBBoxWriter
      extends GeometryGenericWKBBBoxWriter<ByteBuffer> {
    private GeometryByteBufferWKBBBoxWriter(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public void write(int parentRepetition, ByteBuffer buffer) {
      writeByteBuffer(parentRepetition, buffer);
    }
  }

  private static class GeometryJtsWKBBBoxWriter extends GeometryGenericWKBBBoxWriter<Geometry> {
    private GeometryJtsWKBBBoxWriter(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public void write(int parentRepetition, Geometry geom) {
      writeJtsGeometry(parentRepetition, geom);
    }
  }

  public static class GeometryGenericNestedListWriter<T> implements ParquetValueWriter<T> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ColumnWriter<Integer> typeColumnWriter;
    private final ColumnWriter<Double> xColumnWriter;
    private final ColumnWriter<Double> yColumnWriter;
    private final ColumnWriter<Double> zColumnWriter;
    private final ColumnWriter<Double> mColumnWriter;
    private final ColumnWriter<Integer> coordRangesColumnWriter;
    private final ColumnWriter<Integer> lineRangesColumnWriter;
    private final ColumnWriter<Integer> geomRangesColumnWriter;
    private final ColumnWriter<Integer> geomTypesColumnWriter;
    private final List<TripleWriter<?>> children;
    private final GeometryFieldMetrics.GenericBuilder<T> metricsBuilder;

    private final List<Coordinate> coordinates = Lists.newArrayList();
    private final List<Integer> coordRanges = Lists.newArrayList();
    private final List<Integer> lineRanges = Lists.newArrayList();
    private final List<Integer> geomRanges = Lists.newArrayList();
    private final List<Integer> geomTypes = Lists.newArrayList();

    public GeometryGenericNestedListWriter(MessageType type, String[] prefix) {
      ColumnDescriptor typeDesc = type.getColumnDescription(ArrayUtil.add(prefix, "type"));
      ColumnDescriptor xDesc = type.getColumnDescription(ArrayUtil.add(prefix, "x"));
      ColumnDescriptor yDesc = type.getColumnDescription(ArrayUtil.add(prefix, "y"));
      ColumnDescriptor zDesc = type.getColumnDescription(ArrayUtil.add(prefix, "z"));
      ColumnDescriptor mDesc = type.getColumnDescription(ArrayUtil.add(prefix, "m"));
      ColumnDescriptor coordRangeDesc =
          type.getColumnDescription(ArrayUtil.add(prefix, "coordinate_ranges"));
      ColumnDescriptor lineRangeDesc =
          type.getColumnDescription(ArrayUtil.add(prefix, "line_ranges"));
      ColumnDescriptor geomRangeDesc =
          type.getColumnDescription(ArrayUtil.add(prefix, "geometry_ranges"));
      ColumnDescriptor geomTypesDesc =
          type.getColumnDescription(ArrayUtil.add(prefix, "geometry_types"));
      definitionLevel = xDesc.getMaxDefinitionLevel();
      repetitionLevel = xDesc.getMaxRepetitionLevel();

      typeColumnWriter = ColumnWriter.newWriter(typeDesc);
      xColumnWriter = ColumnWriter.newWriter(xDesc);
      yColumnWriter = ColumnWriter.newWriter(yDesc);
      zColumnWriter = ColumnWriter.newWriter(zDesc);
      mColumnWriter = ColumnWriter.newWriter(mDesc);
      coordRangesColumnWriter = ColumnWriter.newWriter(coordRangeDesc);
      lineRangesColumnWriter = ColumnWriter.newWriter(lineRangeDesc);
      geomRangesColumnWriter = ColumnWriter.newWriter(geomRangeDesc);
      geomTypesColumnWriter = ColumnWriter.newWriter(geomTypesDesc);
      children =
          ImmutableList.of(
              typeColumnWriter,
              xColumnWriter,
              yColumnWriter,
              zColumnWriter,
              mColumnWriter,
              coordRangesColumnWriter,
              lineRangesColumnWriter,
              geomRangesColumnWriter,
              geomTypesColumnWriter);

      int id = type.getType(prefix).getId().intValue();
      metricsBuilder = new GeometryFieldMetrics.GenericBuilder<>(id);
    }

    @Override
    public void write(int parentRepetition, T obj) {
      resetState();
      if (obj instanceof ByteBuffer) {
        writeByteBuffer(parentRepetition, (ByteBuffer) obj);
      } else if (obj instanceof Geometry) {
        writeJtsGeometry(parentRepetition, (Geometry) obj);
      } else {
        throw new IllegalArgumentException(
            "Cannot write object of type " + obj.getClass() + " as geometry value");
      }
    }

    protected void writeByteBuffer(int parentRepetition, ByteBuffer buffer) {
      Geometry geom = GeometryUtils.byteBuffer2geometry(buffer);
      writeJtsGeometry(parentRepetition, geom);
    }

    protected void writeJtsGeometry(int parentRepetition, Geometry geom) {
      metricsBuilder.addJtsGeometry(geom);
      if (geom instanceof Point) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbPoint);
        writePoint((Point) geom);
        writeCoordinates(parentRepetition);
        coordRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        lineRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof MultiPoint) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbMultiPoint);
        writeMultiPoint((MultiPoint) geom);
        writeCoordinates(parentRepetition);
        coordRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        lineRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof LineString) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbLineString);
        writeLineString((LineString) geom);
        writeCoordinates(parentRepetition);
        coordRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        lineRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof MultiLineString) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbMultiLineString);
        writeMultiLineString((MultiLineString) geom);
        writeCoordinates(parentRepetition);
        writeCoordRanges(parentRepetition);
        lineRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof Polygon) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbPolygon);
        writePolygon((Polygon) geom);
        writeCoordinates(parentRepetition);
        writeCoordRanges(parentRepetition);
        lineRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof MultiPolygon) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbMultiPolygon);
        writeMultiPolygon((MultiPolygon) geom);
        writeCoordinates(parentRepetition);
        writeCoordRanges(parentRepetition);
        writeLineRanges(parentRepetition);
        geomRangesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        geomTypesColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      } else if (geom instanceof GeometryCollection) {
        typeColumnWriter.write(parentRepetition, WKBConstants.wkbGeometryCollection);
        writeGeometryCollection((GeometryCollection) geom);
        writeCoordinates(parentRepetition);
        writeCoordRanges(parentRepetition);
        writeLineRanges(parentRepetition);
        writeGeomRanges(parentRepetition);
        writeGeomTypes(parentRepetition);
      } else {
        throw new UnsupportedOperationException(
            "Geometry type of " + geom.getGeometryType() + " is not supported");
      }
    }

    private void resetState() {
      coordinates.clear();
      coordRanges.clear();
      lineRanges.clear();
      geomRanges.clear();
      geomTypes.clear();
    }

    private void writeCoordinates(int parentRepetition) {
      int numCoordinates = coordinates.size();
      if (numCoordinates == 0) {
        xColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        yColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        zColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        mColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
        return;
      }

      // scan the coordinates to find if Z/M was present
      boolean hasZ = false;
      boolean hasM = false;
      for (Coordinate coordinate : coordinates) {
        if (!Double.isNaN(coordinate.getZ())) {
          hasZ = true;
        }
        if (!Double.isNaN(coordinate.getM())) {
          hasM = true;
        }
      }

      // write first coordinate
      Coordinate coordinate = coordinates.get(0);
      xColumnWriter.write(parentRepetition, coordinate.x);
      yColumnWriter.write(parentRepetition, coordinate.y);
      if (hasZ) {
        zColumnWriter.write(parentRepetition, coordinate.getZ());
      } else {
        zColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      }
      if (hasM) {
        mColumnWriter.write(parentRepetition, coordinate.getM());
      } else {
        mColumnWriter.writeNull(parentRepetition, definitionLevel - 1);
      }

      // write remaining coordinates
      for (int k = 1; k < numCoordinates; k++) {
        coordinate = coordinates.get(k);
        xColumnWriter.write(repetitionLevel, coordinate.x);
        yColumnWriter.write(repetitionLevel, coordinate.y);
        if (hasZ) {
          zColumnWriter.write(repetitionLevel, coordinate.getZ());
        }
        if (hasM) {
          mColumnWriter.write(repetitionLevel, coordinate.getM());
        }
      }
    }

    private void writeCoordRanges(int parentRepetition) {
      writeIntArray(coordRangesColumnWriter, coordRanges, parentRepetition);
    }

    private void writeLineRanges(int parentRepetition) {
      writeIntArray(lineRangesColumnWriter, lineRanges, parentRepetition);
    }

    private void writeGeomRanges(int parentRepetition) {
      writeIntArray(geomRangesColumnWriter, geomRanges, parentRepetition);
    }

    private void writeGeomTypes(int parentRepetition) {
      writeIntArray(geomTypesColumnWriter, geomTypes, parentRepetition);
    }

    private void writeIntArray(
        ColumnWriter<Integer> columnWriter, List<Integer> elements, int parentRepetition) {
      int total = elements.size();
      if (total == 0) {
        columnWriter.writeNull(parentRepetition, definitionLevel - 1);
        return;
      }

      columnWriter.write(parentRepetition, elements.get(0));
      for (int k = 1; k < total; k++) {
        columnWriter.write(repetitionLevel, elements.get(k));
      }
    }

    private void writePoint(Point geom) {
      Coordinate coordinate = geom.getCoordinate();
      if (coordinate != null) {
        coordinates.add(coordinate);
      }
      coordRanges.add(coordinates.size());
      lineRanges.add(coordRanges.size());
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbPoint);
    }

    private void writeMultiPoint(MultiPoint geom) {
      int numPoints = geom.getNumGeometries();
      for (int k = 0; k < numPoints; k++) {
        Geometry point = geom.getGeometryN(k);
        Coordinate coordinate = point.getCoordinate();
        if (coordinate != null) {
          coordinates.add(coordinate);
        }
      }
      coordRanges.add(coordinates.size());
      lineRanges.add(coordRanges.size());
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbMultiPoint);
    }

    private void writeLineString(LineString geom) {
      this.coordinates.addAll(Arrays.asList(geom.getCoordinates()));
      coordRanges.add(coordinates.size());
      lineRanges.add(coordRanges.size());
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbLineString);
    }

    private void writeMultiLineString(MultiLineString geom) {
      int numLineStrings = geom.getNumGeometries();
      for (int k = 0; k < numLineStrings; k++) {
        Geometry lineString = geom.getGeometryN(k);
        this.coordinates.addAll(Arrays.asList(lineString.getCoordinates()));
        coordRanges.add(coordinates.size());
      }
      lineRanges.add(coordRanges.size());
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbMultiLineString);
    }

    private void writePolygonInternal(Polygon geom) {
      LineString exteriorRing = geom.getExteriorRing();
      this.coordinates.addAll(Arrays.asList(exteriorRing.getCoordinates()));
      coordRanges.add(coordinates.size());
      int numInteriorRings = geom.getNumInteriorRing();
      for (int k = 0; k < numInteriorRings; k++) {
        LineString interiorRing = geom.getInteriorRingN(k);
        this.coordinates.addAll(Arrays.asList(interiorRing.getCoordinates()));
        coordRanges.add(coordinates.size());
      }
      lineRanges.add(coordRanges.size());
    }

    private void writePolygon(Polygon geom) {
      writePolygonInternal(geom);
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbPolygon);
    }

    private void writeMultiPolygon(MultiPolygon geom) {
      int numPolygons = geom.getNumGeometries();
      for (int k = 0; k < numPolygons; k++) {
        Polygon polygon = (Polygon) geom.getGeometryN(k);
        writePolygonInternal(polygon);
      }
      geomRanges.add(lineRanges.size());
      geomTypes.add(WKBConstants.wkbMultiPolygon);
    }

    private void writeGeometryCollection(GeometryCollection geometryCollection) {
      int numGeoms = geometryCollection.getNumGeometries();
      for (int k = 0; k < numGeoms; k++) {
        Geometry geom = geometryCollection.getGeometryN(k);
        if (geom instanceof Point) {
          writePoint((Point) geom);
        } else if (geom instanceof MultiPoint) {
          writeMultiPoint((MultiPoint) geom);
        } else if (geom instanceof LineString) {
          writeLineString((LineString) geom);
        } else if (geom instanceof MultiLineString) {
          writeMultiLineString((MultiLineString) geom);
        } else if (geom instanceof Polygon) {
          writePolygon((Polygon) geom);
        } else if (geom instanceof MultiPolygon) {
          writeMultiPolygon((MultiPolygon) geom);
        } else if (geom instanceof GeometryCollection) {
          throw new UnsupportedOperationException("Nested GeometryCollection is not supported");
        } else {
          throw new UnsupportedOperationException(
              "Geometry type of " + geom.getGeometryType() + " is not supported");
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      typeColumnWriter.setColumnStore(columnStore);
      xColumnWriter.setColumnStore(columnStore);
      yColumnWriter.setColumnStore(columnStore);
      zColumnWriter.setColumnStore(columnStore);
      mColumnWriter.setColumnStore(columnStore);
      coordRangesColumnWriter.setColumnStore(columnStore);
      lineRangesColumnWriter.setColumnStore(columnStore);
      geomRangesColumnWriter.setColumnStore(columnStore);
      geomTypesColumnWriter.setColumnStore(columnStore);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(metricsBuilder.build());
    }
  }

  private static class GeometryByteBufferNestedListWriter
      extends GeometryGenericNestedListWriter<ByteBuffer> {
    GeometryByteBufferNestedListWriter(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public void write(int parentRepetition, ByteBuffer buffer) {
      writeByteBuffer(parentRepetition, buffer);
    }
  }

  private static class GeometryJtsNestedListWriter
      extends GeometryGenericNestedListWriter<Geometry> {
    GeometryJtsNestedListWriter(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public void write(int parentRepetition, Geometry geom) {
      writeJtsGeometry(parentRepetition, geom);
    }
  }
}
