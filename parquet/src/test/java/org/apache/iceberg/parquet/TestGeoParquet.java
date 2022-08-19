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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.TypeUtil.GeometryUtils;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.GeometryBoundType;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

/** Test reading and writing GeoParquet files, which contains records with geometry values. */
public class TestGeoParquet {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final Schema schema =
      new Schema(
          Types.NestedField.required(0, "id", Types.IntegerType.get()),
          Types.NestedField.optional(1, "geom", Types.GeometryType.get()));

  private final String[] geometryJavaTypes = {"jts-geometry", "byte-buffer"};
  private final String[] geometryEncodings = {"wkb", "wkb-bbox", "nested-list"};
  private final List<String> testGeometriesWkt =
      Lists.newArrayList(
          null,
          "POINT EMPTY",
          "POINT(10 20)",
          "LINESTRING EMPTY",
          "LINESTRING(0 0, 0 1, 1 2)",
          "POLYGON EMPTY",
          "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
          "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))",
          "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3))",
          "MULTIPOINT EMPTY",
          "MULTIPOINT(1 2)",
          "MULTIPOINT(1 2, 3 4)",
          "MULTILINESTRING EMPTY",
          "MULTILINESTRING((0 0, 0 1, 1 2))",
          "MULTILINESTRING((0 0, 0 1, 1 2), (1 1, 1 2, 2 2, 2 1))",
          "MULTIPOLYGON EMPTY",
          "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1)), ((0 0, 5 0, 5 5, 0 5, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 3, 3 4, 4 4, 4 3, 3 3)))",
          "GEOMETRYCOLLECTION EMPTY",
          "GEOMETRYCOLLECTION(POINT(10 20), POINT EMPTY, MULTIPOINT(1 2, 3 4), LINESTRING(0 0, 0 1, 1 2), MULTILINESTRING EMPTY, MULTILINESTRING((0 0, 0 1, 1 2), (1 1, 1 2, 2 2, 2 1)))",
          "GEOMETRYCOLLECTION(POLYGON((0 0, 1 0, 1 1, 0 1, 0 0)), MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))), LINESTRING(0 0, 0 1, 1 2))",
          "GEOMETRYCOLLECTION(MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))), MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))))");

  @Test
  public void testWritingAndReadingGeometry() throws IOException {
    for (String geometryEncoding : geometryEncodings) {
      for (String geometryJavaType : geometryJavaTypes) {
        testWriteAndReadGeometryValues(
            geometryEncoding + "_" + geometryJavaType,
            geometryEncoding,
            geometryJavaType,
            testGeometriesWkt);
      }
    }
  }

  @Test
  public void testGeometryBoundsOfEmptyGeometries() throws IOException {
    List<String> emptyGeometriesWkt =
        Lists.newArrayList(
            null,
            "POINT EMPTY",
            "LINESTRING EMPTY",
            "POLYGON EMPTY",
            "MULTIPOLYGON EMPTY",
            "GEOMETRYCOLLECTION EMPTY");
    List<GenericRecord> records = Lists.newArrayList();
    for (int k = 0; k < emptyGeometriesWkt.size(); k++) {
      String wkt = emptyGeometriesWkt.get(k);
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", k);
      record.setField("geom", wkt != null ? GeometryUtils.wkt2geometry(wkt) : null);
      records.add(record);
    }
    for (String geometryEncoding : geometryEncodings) {
      Pair<List<GenericRecord>, Metrics> results =
          writeAndReadRecordsWithGeometryValues(
              "empty_geometry_values_" + geometryEncoding,
              geometryEncoding,
              "jts-geometry",
              records);
      Metrics metrics = results.second();
      Assert.assertNull(
          "Lower bound of empty geometries should be null", metrics.lowerBounds().get(1));
      Assert.assertNull(
          "Upper bound of empty geometries should be null", metrics.upperBounds().get(1));
    }
  }

  private Pair<List<GenericRecord>, Metrics> writeAndReadRecordsWithGeometryValues(
      String desc, String geometryEncoding, String geometryJavaType, List<GenericRecord> records)
      throws IOException {
    File file = temp.newFile(desc + ".parquet");
    file.delete();

    Metrics metrics = writeRecordsWithGeometryValues(file, geometryEncoding, records);

    List<GenericRecord> readRecords = Lists.newArrayList();
    try (CloseableIterable<GenericRecord> readRecordsIter =
        Parquet.read(Files.localInput(file))
            .project(schema)
            .createReaderFunc(
                fileSchema ->
                    GenericParquetReaders.buildReader(
                        schema,
                        fileSchema,
                        ImmutableMap.of(),
                        ImmutableMap.of("read.parquet.geometry.java-type", geometryJavaType)))
            .build()) {
      for (GenericRecord r : readRecordsIter) {
        readRecords.add(r);
      }
    }

    return Pair.of(readRecords, metrics);
  }

  private Metrics writeRecordsWithGeometryValues(
      File file, String geometryEncoding, List<GenericRecord> records) throws IOException {
    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(
                type -> GenericParquetWriter.buildWriter(schema, type, Maps.newHashMap()))
            .set(TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING, geometryEncoding)
            .build();
    Metrics metrics;
    try {
      appender.addAll(records);
    } finally {
      appender.close();
      metrics = appender.metrics();
    }
    return metrics;
  }

  private void testWriteAndReadGeometryValues(
      String desc, String geometryEncoding, String geometryJavaType, List<String> geometryValuesWkt)
      throws IOException {
    List<GenericRecord> records = Lists.newArrayList();
    for (int k = 0; k < geometryValuesWkt.size(); k++) {
      String wkt = geometryValuesWkt.get(k);
      if (wkt == null) {
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", k);
        record.setField("geom", null);
        records.add(record);
      } else {
        Geometry geom = GeometryUtils.wkt2geometry(wkt);
        GenericRecord record = GenericRecord.create(schema);
        record.setField("id", k);
        record.setField("geom", geom);
        records.add(record);
        record = GenericRecord.create(schema);
        record.setField("id", k);
        record.setField("geom", GeometryUtils.geometry2byteBuffer(geom));
        records.add(record);
      }
    }
    Pair<List<GenericRecord>, Metrics> result =
        writeAndReadRecordsWithGeometryValues(desc, geometryEncoding, geometryJavaType, records);
    Metrics metrics = result.second();
    org.apache.iceberg.expressions.Pair<Double, Double> lowerBound =
        Conversions.fromByteBuffer(GeometryBoundType.get(), metrics.lowerBounds().get(1));
    org.apache.iceberg.expressions.Pair<Double, Double> upperBound =
        Conversions.fromByteBuffer(GeometryBoundType.get(), metrics.upperBounds().get(1));
    Envelope envelope =
        new Envelope(
            lowerBound.first(), upperBound.first(), lowerBound.second(), upperBound.second());

    List<GenericRecord> readRecords = result.first();
    Assert.assertEquals("Record count should match", records.size(), readRecords.size());
    for (int k = 0; k < records.size(); k++) {
      GenericRecord expectedRecord = records.get(k);
      GenericRecord actualRecord = readRecords.get(k);
      Assert.assertEquals(
          "Non geometry value should match",
          expectedRecord.getField("id"),
          actualRecord.getField("id"));
      validateGeometryValues(
          geometryJavaType, expectedRecord.getField("geom"), actualRecord.getField("geom"));
      Object expectedGeom = expectedRecord.getField("geom");
      if (expectedGeom instanceof Geometry) {
        Envelope geomEnv = ((Geometry) expectedGeom).getEnvelopeInternal();
        if (!geomEnv.isNull()) {
          Assert.assertTrue(
              "Geometry bounds should contains MBR of all geometries", envelope.contains(geomEnv));
        }
      }
    }
  }

  private void validateGeometryValues(String geometryJavaType, Object expected, Object actual) {
    String expectedWKT = geometryObjectToWKT(expected);
    if (actual == null) {
      Assert.assertNull("Null geometry values should read as null", expected);
    } else if (actual instanceof Geometry) {
      Assert.assertEquals(
          "Geometry values should be read as JTS Geometry object",
          "jts-geometry",
          geometryJavaType);
      Assert.assertEquals(expectedWKT, geometryObjectToWKT(actual));
    } else if (actual instanceof ByteBuffer) {
      Assert.assertEquals(
          "Geometry values should be read as ByteBuffer", "byte-buffer", geometryJavaType);
      Assert.assertEquals(expectedWKT, geometryObjectToWKT(actual));
    } else {
      Assert.fail("Invalid class of geometry object: " + actual.getClass().getName());
    }
  }

  private String geometryObjectToWKT(Object geometryValue) {
    if (geometryValue == null) {
      return null;
    } else if (geometryValue instanceof Geometry) {
      return geometryValue.toString();
    } else if (geometryValue instanceof ByteBuffer) {
      Geometry geometry = GeometryUtils.byteBuffer2geometry((ByteBuffer) geometryValue);
      return geometry.toString();
    } else {
      throw new IllegalArgumentException(
          "Invalid class of geometry object: " + geometryValue.getClass().getName());
    }
  }
}
