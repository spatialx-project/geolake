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

import static org.apache.iceberg.TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.DISJOINT_WINDOWS;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.GEOM_METRIC_WINDOW;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.GEOM_X_MAX;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.GEOM_X_MIN;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.GEOM_Y_MAX;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.GEOM_Y_MIN;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.INSIDE_WINDOWS;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.OVERLAPPED_BUT_NOT_INSIDE_WINDOWS;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.TOUCHED_LINES;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.TOUCHED_POINTS;
import static org.apache.iceberg.expressions.TestGeometryHelpers.MetricEvalData.TOUCHED_WINDOWS;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.GeoParquetUtil;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetDictionaryRowGroupFilter;
import org.apache.iceberg.parquet.ParquetIO;
import org.apache.iceberg.parquet.ParquetMetricsRowGroupFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestGeoParquetRowGroupFilter {
  private static final Logger LOG = LoggerFactory.getLogger(TestGeoParquetRowGroupFilter.class);
  private static final String geometryName = "geom";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, geometryName, Types.GeometryType.get()));
  private static final TableIdentifier tableName = TableIdentifier.of("geo_parquet");
  public static final Configuration conf = new Configuration();
  private static File warehouse = null;
  private static String warehousePath = "";
  private static Table table = null;
  private static final Point[] points =
      new Point[] {
        new GeometryFactory().createPoint(new Coordinate(GEOM_X_MIN, GEOM_Y_MIN)),
        new GeometryFactory().createPoint(new Coordinate(GEOM_X_MAX, GEOM_Y_MAX))
      };
  private static final int N = 20;
  private final String encoding;
  private String dataFilePath;
  private MessageType fileSchema;
  private BlockMetaData rowGroup;
  private ParquetFileReader reader;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  public TestGeoParquetRowGroupFilter(String encoding) {
    this.encoding = encoding;
  }

  @Parameterized.Parameters(name = "encoding = {0}")
  public static Object[] parameters() {
    return new Object[] {"wkb-bbox", "nested-list", "wkb"};
  }

  @Before
  public void createInputFile() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
    warehousePath = (new Path(warehouse.getAbsolutePath())).toString();
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    if (!catalog.tableExists(tableName)) {
      Map<String, String> properties = Maps.newHashMap();
      properties.put(PARQUET_GEOMETRY_WRITE_ENCODING, encoding);
      table = catalog.createTable(tableName, SCHEMA, null, properties);
      dataFilePath = String.format(warehousePath + "/%s.parquet", encoding);
      OutputFile outputFile = table.io().newOutputFile(dataFilePath);
      DataWriter<Object> writer =
          Parquet.writeData(outputFile)
              .forTable(table)
              .createWriterFunc(
                  type ->
                      GenericParquetWriter.buildWriter(table.schema(), type, table.properties()))
              .build();
      for (int k = 0; k < N; k++) {
        Record record = GenericRecord.create(table.schema());
        record.set(0, k);
        // the `geom` field only have 2 distinct values, so parquet file will have a dictionary page
        record.set(1, k > N / 2 ? points[0] : points[1]);
        writer.write(record);
      }
      writer.close();
      DataFile dataFile = writer.toDataFile();
      LOG.info("append data file: " + dataFilePath);
      table.newAppend().appendFile(dataFile).commit();
    }
    prepare();
  }

  private void prepare() throws IOException {
    InputFile inputFile = Files.localInput(dataFilePath);
    reader = ParquetFileReader.open(ParquetIO.file(inputFile));
    rowGroup = reader.getRowGroups().get(0);
    fileSchema = reader.getFileMetaData().getSchema();
    Types.NestedField geoField = SCHEMA.findField(2);
    Type geoType = GeoParquetUtil.geomFieldFromFileSchema(fileSchema, geoField);
  }

  @After
  public void clear() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path path = new Path(warehouse.getAbsolutePath());
      FileSystem fs = path.getFileSystem(conf);
      Assert.assertTrue("Failed to delete " + warehousePath, fs.delete(path, true));
    }
  }

  private boolean shouldReadByMetric(UnboundPredicate<Geometry> pred) {
    return new ParquetMetricsRowGroupFilter(SCHEMA, pred).shouldRead(fileSchema, rowGroup);
  }

  private boolean shouldReadByDictionary(UnboundPredicate<Geometry> pred) {
    return new ParquetDictionaryRowGroupFilter(SCHEMA, pred)
        .shouldRead(fileSchema, rowGroup, reader.getDictionaryReader(rowGroup));
  }

  @Test
  public void testStIntersectsImpInMetricFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(
                TOUCHED_POINTS,
                TOUCHED_LINES,
                TOUCHED_WINDOWS,
                INSIDE_WINDOWS,
                OVERLAPPED_BUT_NOT_INSIDE_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByMetric(Expressions.stIntersects(geometryName, queryWindow));
          Assert.assertTrue(
              String.format(
                  "Should read: data in %s may intersects with %s",
                  GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
              shouldRead);
        });

    for (Geometry queryWindow : DISJOINT_WINDOWS) {
      boolean shouldRead = shouldReadByMetric(Expressions.stIntersects(geometryName, queryWindow));
      if (!"wkb".equals(encoding)) {
        Assert.assertFalse(
            String.format(
                "Should not read: data in %s can not intersects with %s",
                GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
            shouldRead);
      } else {
        Assert.assertTrue(
            "Should read: file with `wkb` encoding does not have valid row group metrics of geometry field",
            shouldRead);
      }
    }
  }

  @Test
  public void testStCoveredByImpInMetricFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(
                TOUCHED_POINTS,
                TOUCHED_LINES,
                TOUCHED_WINDOWS,
                INSIDE_WINDOWS,
                OVERLAPPED_BUT_NOT_INSIDE_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByMetric(Expressions.stCoveredBy(geometryName, queryWindow));
          Assert.assertTrue(
              String.format(
                  "Should read: data in %s may coveredBy %s",
                  GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
              shouldRead);
        });

    for (Geometry queryWindow : DISJOINT_WINDOWS) {
      boolean shouldRead = shouldReadByMetric(Expressions.stCoveredBy(geometryName, queryWindow));
      if (!"wkb".equals(encoding)) {
        Assert.assertFalse(
            String.format(
                "Should not read: data in %s can not coveredBy %s",
                GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
            shouldRead);
      } else {
        Assert.assertTrue(
            "Should read: file with `wkb` encoding does not have valid row group metrics of geometry field",
            shouldRead);
      }
    }
  }

  @Test
  public void testStCoversImpInMetricFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(DISJOINT_WINDOWS, OVERLAPPED_BUT_NOT_INSIDE_WINDOWS, TOUCHED_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead = shouldReadByMetric(Expressions.stCovers(geometryName, queryWindow));
          if (!"wkb".equals(encoding)) {
            Assert.assertFalse(
                String.format(
                    "Should not read: data in %s can not covers %s",
                    GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
                shouldRead);
          } else {
            Assert.assertTrue(
                "Should read: file with `wkb` encoding does not have valid row group metrics of geometry field",
                shouldRead);
          }
        });

    geometryStream = Stream.of(TOUCHED_POINTS, TOUCHED_LINES, INSIDE_WINDOWS).flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead = shouldReadByMetric(Expressions.stCovers(geometryName, queryWindow));
          Assert.assertTrue(
              String.format(
                  "Should read: data in %s may covers %s",
                  GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
              shouldRead);
        });
  }

  @Test
  public void testStIntersectsImpInDictionaryFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(
                TOUCHED_POINTS,
                TOUCHED_LINES,
                TOUCHED_WINDOWS,
                INSIDE_WINDOWS,
                OVERLAPPED_BUT_NOT_INSIDE_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByDictionary(Expressions.stIntersects(geometryName, queryWindow));
          if (!"nested-list".equals(encoding)) {
            Assert.assertEquals(
                Arrays.stream(points).anyMatch(p -> p.intersects(queryWindow)), shouldRead);
          } else {
            Assert.assertTrue(
                "Should read: file with `nested-list` encoding does not have valid dictionary of geometry field",
                shouldRead);
          }
        });

    for (Geometry queryWindow : DISJOINT_WINDOWS) {
      boolean shouldRead =
          shouldReadByDictionary(Expressions.stIntersects(geometryName, queryWindow));
      if (!"nested-list".equals(encoding)) {
        Assert.assertFalse(
            String.format(
                "Should not read: data in %s can not within %s",
                GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
            shouldRead);
      } else {
        Assert.assertTrue(
            "Should read: file with `nested-list` encoding does not have valid dictionary of geometry field",
            shouldRead);
      }
    }
  }

  @Test
  public void testStCoveredByImpInDictionaryFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(
                TOUCHED_POINTS,
                TOUCHED_LINES,
                TOUCHED_WINDOWS,
                INSIDE_WINDOWS,
                OVERLAPPED_BUT_NOT_INSIDE_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByDictionary(Expressions.stCoveredBy(geometryName, queryWindow));
          if (!"nested-list".equals(encoding)) {
            Assert.assertEquals(
                Arrays.stream(points).anyMatch(p -> p.coveredBy(queryWindow)), shouldRead);
          } else {
            Assert.assertTrue(
                "Should read: file with `nested-list` encoding does not have valid dictionary of geometry field",
                shouldRead);
          }
        });

    for (Geometry queryWindow : DISJOINT_WINDOWS) {
      boolean shouldRead =
          shouldReadByDictionary(Expressions.stCoveredBy(geometryName, queryWindow));
      if ("nested-list".equals(encoding)) {
        Assert.assertTrue(
            "Should read: file with `nested-list` encoding does not have valid dictionary of geometry field",
            shouldRead);
      } else {
        Assert.assertFalse(
            String.format(
                "Should not read: data in %s can not coveredBy %s",
                GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
            shouldRead);
      }
    }
  }

  @Test
  public void testStCoversImpInDictionaryFilter() {
    Stream<Geometry> geometryStream =
        Stream.of(DISJOINT_WINDOWS, OVERLAPPED_BUT_NOT_INSIDE_WINDOWS, TOUCHED_WINDOWS)
            .flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByDictionary(Expressions.stCovers(geometryName, queryWindow));
          if (!"nested-list".equals(encoding)) {
            Assert.assertFalse(
                String.format(
                    "Should not read: data in %s can not covers %s",
                    GEOM_METRIC_WINDOW, queryWindow.getEnvelopeInternal()),
                shouldRead);
          } else {
            Assert.assertTrue(
                "Should read: file with `nested-list` encoding does not have valid row group metrics of geometry field",
                shouldRead);
          }
        });

    geometryStream = Stream.of(TOUCHED_POINTS, TOUCHED_LINES, INSIDE_WINDOWS).flatMap(Stream::of);
    geometryStream.forEach(
        queryWindow -> {
          boolean shouldRead =
              shouldReadByDictionary(Expressions.stCovers(geometryName, queryWindow));
          if (!"nested-list".equals(encoding)) {
            Assert.assertEquals(
                Arrays.stream(points).anyMatch(p -> p.covers(queryWindow)), shouldRead);
          } else {
            Assert.assertTrue(
                "Should read: file with `nested-list` encoding does not have valid dictionary of geometry field",
                shouldRead);
          }
        });
  }
}
