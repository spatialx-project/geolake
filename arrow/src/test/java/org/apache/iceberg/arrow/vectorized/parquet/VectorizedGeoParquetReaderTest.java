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
package org.apache.iceberg.arrow.vectorized.parquet;

import static org.apache.iceberg.Files.localInput;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.arrow.vectorized.ArrowGeometryVectorAccessor;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.arrow.vectorized.VectorHolder.GeometryVectorHolder;
import org.apache.iceberg.arrow.vectorized.VectorizedTableScanIterable;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

/** Test reading GeoParquet file using vectorized reader. */
public class VectorizedGeoParquetReaderTest {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final HadoopTables tables = new HadoopTables();

  private final Schema schema =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "geom", Types.GeometryType.get()));

  private final String[] geometryEncodings = {"wkb", "wkb-bbox", "nested-list"};

  private final Random random = new Random();
  private final GeometryFactory ff = new GeometryFactory();

  @Test
  public void testWriteAndReadRandomGeometries() throws IOException {
    List<GenericRecord> records = generateRandomRecords(1000);
    for (String geometryEncoding : geometryEncodings) {
      String tableLocation =
          writeRecordsToTempTable("vectorized_" + geometryEncoding, geometryEncoding, records);
      List<GenericRecord> readRecords = readRecordsFromTempTable(tableLocation, 18);
      Assert.assertEquals("Number of records should match", records.size(), readRecords.size());
      for (int i = 0; i < records.size(); i++) {
        Assert.assertEquals(records.get(i).getField("id"), readRecords.get(i).getField("id"));
        Assert.assertEquals(records.get(i).getField("geom"), readRecords.get(i).getField("geom"));
      }
    }
  }

  @Test
  public void testWriteAndReadNestedListGeometriesWithVariousBatchSizes() throws IOException {
    List<GenericRecord> records = generateRandomRecords(10000);
    testWriteAndReadNestedListGeometries("small_batch_size", records, 1, 20);
    testWriteAndReadNestedListGeometries("batch_size_spans_multiple_pages", records, 11000, 11010);
  }

  @Test
  public void testWriteAndReadNestedListGeometriesAllNulls() throws IOException {
    int count = 200000;
    List<GenericRecord> records = Lists.newArrayListWithCapacity(count);
    for (int k = 0; k < count; k++) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", k);
      record.setField("geom", null);
      records.add(record);
    }
    testWriteAndReadNestedListGeometries("null_small_batch_size", records, 1, 20);
    testWriteAndReadNestedListGeometries(
        "null_batch_size_spans_multiple_pages", records, 11000, 11010);
  }

  private void testWriteAndReadNestedListGeometries(
      String desc, List<GenericRecord> records, int batchSizeBegin, int batchSizeEnd)
      throws IOException {
    // In this setup, the vectorized reader may assemble a batch using records from multiple
    // consecutive pages.
    Map<String, String> configs =
        ImmutableMap.of(
            TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "100000",
            TableProperties.PARQUET_PAGE_SIZE_BYTES, "10000");
    String tableLocation = writeRecordsToTempTable(desc, "nested-list", records, configs);
    for (int batchSize = batchSizeBegin; batchSize < batchSizeEnd; batchSize++) {
      List<GenericRecord> readRecords = readRecordsFromTempTable(tableLocation, batchSize);
      Assert.assertEquals("Number of records should match", records.size(), readRecords.size());
      for (int i = 0; i < records.size(); i++) {
        Assert.assertEquals(records.get(i).getField("id"), readRecords.get(i).getField("id"));
        Assert.assertEquals(records.get(i).getField("geom"), readRecords.get(i).getField("geom"));
      }
    }
  }

  private String writeRecordsToTempTable(
      String desc,
      String geometryEncoding,
      List<GenericRecord> records,
      Map<String, String> configs)
      throws IOException {
    File file = temp.newFile();
    file.delete();
    FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(file))
            .schema(schema)
            .createWriterFunc(
                type -> GenericParquetWriter.buildWriter(schema, type, Maps.newHashMap()))
            .set(TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING, geometryEncoding)
            .setAll(configs)
            .build();
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    String tableLocation = temp.newFolder(desc).toString();
    Table table = tables.create(schema, tableLocation);
    OverwriteFiles overwrite = table.newOverwrite();
    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withInputFile(localInput(file))
            .withMetrics(appender.metrics())
            .withFormat(FileFormat.PARQUET)
            .build();
    overwrite.addFile(dataFile);
    overwrite.commit();
    return tableLocation;
  }

  private String writeRecordsToTempTable(
      String desc, String geometryEncoding, List<GenericRecord> records) throws IOException {
    return writeRecordsToTempTable(desc, geometryEncoding, records, ImmutableMap.of());
  }

  private List<GenericRecord> readRecordsFromTempTable(String tableLocation, int batchSize)
      throws IOException {
    Table table = tables.load(tableLocation);
    List<GenericRecord> records = Lists.newArrayList();
    try (VectorizedTableScanIterable itr =
        new VectorizedTableScanIterable(table.newScan(), batchSize, true)) {
      for (ColumnarBatch batch : itr) {
        ColumnVector idVector = batch.column(0);
        ColumnVector geomVector = batch.column(1);
        List<Geometry> geometries = columnVectorToGeometryList(geomVector);
        for (int k = 0; k < batch.numRows(); k++) {
          GenericRecord record = GenericRecord.create(schema);
          record.setField("id", idVector.getInt(k));
          record.setField("geom", geometries.get(k));
          records.add(record);
        }
      }
    }
    return records;
  }

  private List<Geometry> columnVectorToGeometryList(ColumnVector geomVector) {
    Assert.assertTrue(
        "Column vector should be a geometry vector",
        geomVector.getVectorHolder() instanceof GeometryVectorHolder);
    GeometryVectorHolder geomVectorHolder = (GeometryVectorHolder) geomVector.getVectorHolder();
    List<Geometry> geometries = null;
    switch (geomVectorHolder.getGeometryVectorEncoding()) {
      case "wkb":
        VarBinaryVector wkbVector = (VarBinaryVector) geomVector.getFieldVector();
        geometries = wkbVectorToGeometryList(wkbVector);
        break;
      case "wkb-bbox":
        StructVector wkbBBOXVector = (StructVector) geomVector.getFieldVector();
        geometries = wkbVectorToGeometryList((VarBinaryVector) wkbBBOXVector.getChild("wkb"));
        break;
      case "nested-list":
        geometries = nestedListVectorToGeometryList((StructVector) geomVector.getFieldVector());
        break;
      default:
        Assert.fail(
            "Unknown vectorized geometry encoding " + geomVectorHolder.getGeometryVectorEncoding());
    }
    return geometries;
  }

  private List<Geometry> wkbVectorToGeometryList(VarBinaryVector wkbVector) {
    int valueCount = wkbVector.getValueCount();
    List<Geometry> geometries = Lists.newArrayListWithCapacity(valueCount);
    for (int k = 0; k < valueCount; k++) {
      if (wkbVector.isNull(k)) {
        geometries.add(null);
      } else {
        byte[] wkb = wkbVector.get(k);
        try {
          Geometry geometry = new WKBReader().read(wkb);
          geometries.add(geometry);
        } catch (ParseException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return geometries;
  }

  private List<Geometry> nestedListVectorToGeometryList(StructVector vec) {
    int valueCount = vec.getValueCount();
    List<Geometry> geometries = Lists.newArrayListWithCapacity(valueCount);
    ArrowGeometryVectorAccessor.NestedListAccessor accessor =
        new ArrowGeometryVectorAccessor.NestedListAccessor(vec);
    for (int k = 0; k < valueCount; k++) {
      if (vec.isNull(k)) {
        geometries.add(null);
      } else {
        geometries.add(accessor.getGeometry(k));
      }
    }
    return geometries;
  }

  private List<GenericRecord> generateRandomRecords(int count) {
    List<Geometry> geometries = generateRandomGeometries(count);
    List<GenericRecord> records = Lists.newArrayListWithCapacity(count);
    for (int k = 0; k < count; k++) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", k);
      record.setField("geom", geometries.get(k));
      records.add(record);
    }
    return records;
  }

  private List<Geometry> generateRandomGeometries(int count) {
    List<Geometry> geometries = Lists.newArrayListWithCapacity(count);
    for (int k = 0; k < count; k++) {
      Geometry geometry = generateRandomGeometry();
      geometries.add(geometry);
    }
    return geometries;
  }

  private Geometry generateRandomGeometry() {
    switch (random.nextInt(6)) {
      case 1:
        return ff.createPoint(new Coordinate(random.nextDouble(), random.nextDouble()));
      case 2:
        return ff.createPoint(
            new CoordinateXYM(random.nextDouble(), random.nextDouble(), random.nextDouble()));
      case 3:
        return ff.createPoint(
            new CoordinateXYZM(
                random.nextDouble(),
                random.nextDouble(),
                random.nextDouble(),
                random.nextDouble()));
      case 4:
        return ff.createPoint();
      case 5:
        int numElems = random.nextInt(10);
        Geometry[] geoms = new Geometry[numElems];
        for (int k = 0; k < numElems; k++) {
          geoms[k] =
              ff.createPoint(
                  new Coordinate(random.nextDouble(), random.nextDouble(), random.nextDouble()));
        }
        return ff.createGeometryCollection(geoms);
      default:
        return null;
    }
  }
}
