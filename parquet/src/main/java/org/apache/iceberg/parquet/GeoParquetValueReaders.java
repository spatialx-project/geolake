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
import java.util.List;
import org.apache.iceberg.parquet.GeoParquetEnums.GeometryValueType;
import org.apache.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.TypeUtil.GeometryUtils;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * Geometry value readers for GeoParquet, which is a variant of parquet for storing geospatial data.
 */
public class GeoParquetValueReaders {

  private GeoParquetValueReaders() {}

  public static ParquetValueReader<?> createGeometryWKBReader(
      ColumnDescriptor desc, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case BYTE_BUFFER:
        return new ParquetValueReaders.BytesReader(desc);
      case JTS_GEOMETRY:
        return new GeoParquetValueReaders.GeometryJtsWKBReader(desc);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  public static ParquetValueReader<?> createGeometryWKBBBoxReader(
      MessageType type, String[] prefix, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case BYTE_BUFFER:
        return new GeoParquetValueReaders.GeometryByteBufferWKBBBoxReader(type, prefix);
      case JTS_GEOMETRY:
        return new GeoParquetValueReaders.GeometryJtsWKBBBoxReader(type, prefix);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  public static ParquetValueReader<?> createGeometryNestedListReader(
      MessageType type, String[] prefix, GeometryValueType geometryJavaType) {
    switch (geometryJavaType) {
      case BYTE_BUFFER:
        return new GeoParquetValueReaders.GeometryByteBufferNestedListReader(type, prefix);
      case JTS_GEOMETRY:
        return new GeoParquetValueReaders.GeometryJtsNestedListReader(type, prefix);
      default:
        throw new UnsupportedOperationException(
            "Unsupported geometry value type: " + geometryJavaType);
    }
  }

  private static class GeometryJtsWKBReader extends PrimitiveReader<Geometry> {
    GeometryJtsWKBReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Geometry read(Geometry reuse) {
      Binary binary = column.nextBinary();
      ByteBuffer buffer = binary.toByteBuffer();
      return GeometryUtils.byteBuffer2geometry(buffer);
    }
  }

  public abstract static class GeometryGenericWKBBBoxReader<T> implements ParquetValueReader<T> {
    private final int definitionLevel;

    private final ColumnDescriptor wkbDesc;
    private final ColumnDescriptor minXDesc;
    private final ColumnDescriptor minYDesc;
    private final ColumnDescriptor maxXDesc;
    private final ColumnDescriptor maxYDesc;

    private final ColumnIterator<Binary> wkbColumn;
    private final ColumnIterator<Double> minXColumn;
    private final ColumnIterator<Double> minYColumn;
    private final ColumnIterator<Double> maxXColumn;
    private final ColumnIterator<Double> maxYColumn;
    private final List<TripleIterator<?>> children;

    private final GeometryFactory ff = new GeometryFactory();

    public GeometryGenericWKBBBoxReader(MessageType type, String[] prefix) {
      wkbDesc = type.getColumnDescription(ArrayUtil.add(prefix, "wkb"));
      minXDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_x"));
      minYDesc = type.getColumnDescription(ArrayUtil.add(prefix, "min_y"));
      maxXDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_x"));
      maxYDesc = type.getColumnDescription(ArrayUtil.add(prefix, "max_y"));
      definitionLevel = minXDesc.getMaxDefinitionLevel() - 1;

      String writerVersion = "";
      wkbColumn = ColumnIterator.newIterator(wkbDesc, writerVersion);
      minXColumn = ColumnIterator.newIterator(minXDesc, writerVersion);
      minYColumn = ColumnIterator.newIterator(minYDesc, writerVersion);
      maxXColumn = ColumnIterator.newIterator(maxXDesc, writerVersion);
      maxYColumn = ColumnIterator.newIterator(maxYDesc, writerVersion);
      children = ImmutableList.of(wkbColumn, minXColumn, minYColumn, maxXColumn, maxYColumn);
    }

    public Binary readWKB() {
      Binary wkb = wkbColumn.nextBinary();
      if (minXColumn.currentDefinitionLevel() > definitionLevel) {
        double minX = minXColumn.nextDouble();
        double maxX = minYColumn.nextDouble();
        double minY = maxXColumn.nextDouble();
        double maxY = maxYColumn.nextDouble();
        // TODO: determine if this row should be dropped using bounding boxes, and propagate
        // data skipping to main struct reader
      } else {
        minXColumn.nextNull();
        minYColumn.nextNull();
        maxXColumn.nextNull();
        maxYColumn.nextNull();
      }
      return wkb;
    }

    @Override
    public TripleIterator<?> column() {
      return wkbColumn;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @Override
    public void setPageSource(PageReadStore pageStore, long rowPosition) {
      wkbColumn.setPageSource(pageStore.getPageReader(wkbDesc));
      minXColumn.setPageSource(pageStore.getPageReader(minXDesc));
      minYColumn.setPageSource(pageStore.getPageReader(minYDesc));
      maxXColumn.setPageSource(pageStore.getPageReader(maxXDesc));
      maxYColumn.setPageSource(pageStore.getPageReader(maxYDesc));
    }
  }

  private static class GeometryByteBufferWKBBBoxReader
      extends GeometryGenericWKBBBoxReader<ByteBuffer> {
    GeometryByteBufferWKBBBoxReader(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public ByteBuffer read(ByteBuffer reuse) {
      Binary binary = readWKB();
      ByteBuffer data = binary.toByteBuffer();
      if (reuse != null && reuse.hasArray() && reuse.capacity() >= data.remaining()) {
        data.get(reuse.array(), reuse.arrayOffset(), data.remaining());
        reuse.position(0);
        reuse.limit(binary.length());
        return reuse;
      } else {
        byte[] array = new byte[data.remaining()];
        data.get(array, 0, data.remaining());
        return ByteBuffer.wrap(array);
      }
    }
  }

  private static class GeometryJtsWKBBBoxReader extends GeometryGenericWKBBBoxReader<Geometry> {
    GeometryJtsWKBBBoxReader(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public Geometry read(Geometry reuse) {
      Binary binary = readWKB();
      return GeometryUtils.byteBuffer2geometry(binary.toByteBuffer());
    }
  }

  public abstract static class GeometryGenericNestedListReader<T> implements ParquetValueReader<T> {
    private final int definitionLevel;
    private final int repetitionLevel;

    private final ColumnDescriptor typeDesc;
    private final ColumnDescriptor xDesc;
    private final ColumnDescriptor yDesc;
    private final ColumnDescriptor zDesc;
    private final ColumnDescriptor mDesc;
    private final ColumnDescriptor coordRangeDesc;
    private final ColumnDescriptor lineRangeDesc;
    private final ColumnDescriptor geomRangeDesc;
    private final ColumnDescriptor geomTypesDesc;

    private final ColumnIterator<Integer> typeColumn;
    private final ColumnIterator<Double> xColumn;
    private final ColumnIterator<Double> yColumn;
    private final ColumnIterator<Double> zColumn;
    private final ColumnIterator<Double> mColumn;
    private final ColumnIterator<Integer> coordRangesColumn;
    private final ColumnIterator<Integer> lineRangesColumn;
    private final ColumnIterator<Integer> geomRangesColumn;
    private final ColumnIterator<Integer> geomTypesColumn;

    private final List<TripleIterator<?>> children;

    private final GeometryFactory ff = new GeometryFactory();

    public GeometryGenericNestedListReader(MessageType type, String[] prefix) {
      typeDesc = type.getColumnDescription(ArrayUtil.add(prefix, "type"));
      xDesc = type.getColumnDescription(ArrayUtil.add(prefix, "x"));
      yDesc = type.getColumnDescription(ArrayUtil.add(prefix, "y"));
      zDesc = type.getColumnDescription(ArrayUtil.add(prefix, "z"));
      mDesc = type.getColumnDescription(ArrayUtil.add(prefix, "m"));
      coordRangeDesc = type.getColumnDescription(ArrayUtil.add(prefix, "coordinate_ranges"));
      lineRangeDesc = type.getColumnDescription(ArrayUtil.add(prefix, "line_ranges"));
      geomRangeDesc = type.getColumnDescription(ArrayUtil.add(prefix, "geometry_ranges"));
      geomTypesDesc = type.getColumnDescription(ArrayUtil.add(prefix, "geometry_types"));
      definitionLevel = xDesc.getMaxDefinitionLevel() - 1;
      repetitionLevel = xDesc.getMaxRepetitionLevel() - 1;

      String writerVersion = "";
      typeColumn = ColumnIterator.newIterator(typeDesc, writerVersion);
      xColumn = ColumnIterator.newIterator(xDesc, writerVersion);
      yColumn = ColumnIterator.newIterator(yDesc, writerVersion);
      zColumn = ColumnIterator.newIterator(zDesc, writerVersion);
      mColumn = ColumnIterator.newIterator(mDesc, writerVersion);
      coordRangesColumn = ColumnIterator.newIterator(coordRangeDesc, writerVersion);
      lineRangesColumn = ColumnIterator.newIterator(lineRangeDesc, writerVersion);
      geomRangesColumn = ColumnIterator.newIterator(geomRangeDesc, writerVersion);
      geomTypesColumn = ColumnIterator.newIterator(geomTypesDesc, writerVersion);
      children =
          ImmutableList.of(
              typeColumn,
              xColumn,
              yColumn,
              zColumn,
              mColumn,
              coordRangesColumn,
              lineRangesColumn,
              geomRangesColumn,
              geomTypesColumn);
    }

    public Geometry readGeometry() {
      int type = typeColumn.nextInteger();
      List<Coordinate> coordinates = readCoordinates();
      List<Integer> coordRanges = readCoordRanges();
      List<Integer> lineRanges = readLineRanges();
      List<Integer> geomRanges = readGeomRanges();
      List<Integer> geomTypes = readGeomTypes();
      return new GeoParquetGeometryBuilder.NestedListGeometryBuilder(
              ff, type, coordinates, coordRanges, lineRanges, geomRanges, geomTypes)
          .build();
    }

    private List<Coordinate> readCoordinates() {
      List<Coordinate> coordinates = Lists.newArrayList();
      List<Double> zCoordinates = readList(zColumn);
      List<Double> mCoordinates = readList(mColumn);
      boolean hasZ = !zCoordinates.isEmpty();
      boolean hasM = !mCoordinates.isEmpty();
      int coordIndex = 0;
      do {
        if (xColumn.currentDefinitionLevel() > definitionLevel) {
          double coordX = xColumn.nextDouble();
          double coordY = yColumn.nextDouble();
          Coordinate coordinate;
          if (hasZ && hasM) {
            coordinate =
                new CoordinateXYZM(
                    coordX, coordY, zCoordinates.get(coordIndex), mCoordinates.get(coordIndex));
          } else if (hasZ) {
            coordinate = new Coordinate(coordX, coordY, zCoordinates.get(coordIndex));
          } else if (hasM) {
            coordinate = new CoordinateXYM(coordX, coordY, mCoordinates.get(coordIndex));
          } else {
            coordinate = new CoordinateXY(coordX, coordY);
          }
          coordinates.add(coordinate);
          coordIndex += 1;
        } else {
          xColumn.nextNull();
          yColumn.nextNull();
          break;
        }
      } while (xColumn.currentRepetitionLevel() > repetitionLevel);
      return coordinates;
    }

    private <E> List<E> readList(ColumnIterator<E> column) {
      List<E> data = Lists.newArrayList();
      do {
        if (column.currentDefinitionLevel() > definitionLevel) {
          data.add(column.next());
        } else {
          column.nextNull();
          break;
        }
      } while (column.currentRepetitionLevel() > repetitionLevel);
      return data;
    }

    private List<Integer> readCoordRanges() {
      return readList(coordRangesColumn);
    }

    private List<Integer> readLineRanges() {
      return readList(lineRangesColumn);
    }

    private List<Integer> readGeomRanges() {
      return readList(geomRangesColumn);
    }

    private List<Integer> readGeomTypes() {
      return readList(geomTypesColumn);
    }

    @Override
    public TripleIterator<?> column() {
      return typeColumn;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @Override
    public void setPageSource(PageReadStore pageStore, long rowPosition) {
      typeColumn.setPageSource(pageStore.getPageReader(typeDesc));
      xColumn.setPageSource(pageStore.getPageReader(xDesc));
      yColumn.setPageSource(pageStore.getPageReader(yDesc));
      zColumn.setPageSource(pageStore.getPageReader(zDesc));
      mColumn.setPageSource(pageStore.getPageReader(mDesc));
      coordRangesColumn.setPageSource(pageStore.getPageReader(coordRangeDesc));
      lineRangesColumn.setPageSource(pageStore.getPageReader(lineRangeDesc));
      geomRangesColumn.setPageSource(pageStore.getPageReader(geomRangeDesc));
      geomTypesColumn.setPageSource(pageStore.getPageReader(geomTypesDesc));
    }
  }

  private static class GeometryByteBufferNestedListReader
      extends GeometryGenericNestedListReader<ByteBuffer> {
    GeometryByteBufferNestedListReader(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public ByteBuffer read(ByteBuffer reuse) {
      Geometry geom = readGeometry();
      return GeometryUtils.geometry2byteBuffer(geom);
    }
  }

  private static class GeometryJtsNestedListReader
      extends GeometryGenericNestedListReader<Geometry> {
    GeometryJtsNestedListReader(MessageType type, String[] prefix) {
      super(type, prefix);
    }

    @Override
    public Geometry read(Geometry reuse) {
      return readGeometry();
    }
  }
}
