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

import static org.apache.iceberg.TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Map;
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
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
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
import org.locationtech.jts.geom.Envelope;
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
  private static HadoopCatalog catalog = null;
  private static Table table = null;
  private static final int N = 20;
  private final String encoding;
  private String dataFilePath = "";

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
    catalog = new HadoopCatalog(conf, warehousePath);
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
      GeometryFactory ff = new GeometryFactory();
      Point pt = ff.createPoint(new Coordinate(0, 0));
      for (int k = 0; k < N; k++) {
        Record record = GenericRecord.create(table.schema());
        record.set(0, k);
        // the `geom` field only have 2 distinct values, so parquet file will have a dictionary page
        if (k > N / 2) {
          pt = ff.createPoint(new Coordinate(N, N));
        }
        record.set(1, pt);
        writer.write(record);
      }
      writer.close();
      DataFile dataFile = writer.toDataFile();
      LOG.info("append data file: " + dataFilePath);
      table.newAppend().appendFile(dataFile).commit();
    }
  }

  @After
  public void clear() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path path = new Path(warehouse.getAbsolutePath());
      FileSystem fs = path.getFileSystem(conf);
      Assert.assertTrue("Failed to delete " + warehousePath, fs.delete(path, true));
    }
  }

  @Test
  public void testMetricFilter() throws IOException {
    InputFile inputFile = Files.localInput(dataFilePath);
    ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile));
    BlockMetaData rowGroup = reader.getRowGroups().get(0);
    MessageType parquetSchema = reader.getFileMetaData().getSchema();
    Types.NestedField geoField = SCHEMA.findField(2);
    Type type = GeoParquetUtil.geomFieldFromFileSchema(parquetSchema, geoField);

    Geometry disjointPoint = TypeUtil.GeometryUtils.wkt2geometry("POINT (-10 -10)");
    UnboundPredicate<Geometry> exp = Expressions.stWithin(geometryName, disjointPoint);
    ParquetMetricsRowGroupFilter metricRowGroupFilter =
        new ParquetMetricsRowGroupFilter(SCHEMA, exp);
    ParquetDictionaryRowGroupFilter dictionaryRowGroupFilter =
        new ParquetDictionaryRowGroupFilter(SCHEMA, exp);

    switch (encoding) {
      case "wkb":
        Assert.assertTrue(type.isPrimitive());
        // file with "wkb" encoding does not have valid row group metrics of geometry field
        Assert.assertTrue(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));
        Assert.assertFalse(
            dictionaryRowGroupFilter.shouldRead(
                parquetSchema, rowGroup, reader.getDictionaryReader(rowGroup)));
        break;
      case "wkb-bbox":
        Assert.assertFalse(type.isPrimitive());
        Assert.assertFalse(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));
        Assert.assertFalse(
            dictionaryRowGroupFilter.shouldRead(
                parquetSchema, rowGroup, reader.getDictionaryReader(rowGroup)));
        break;
      case "nested-list":
        Assert.assertFalse(type.isPrimitive());
        Assert.assertFalse(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));
        // file with "nested-list" encoding does not have a valid dictionary of geometry objects
        Assert.assertTrue(
            dictionaryRowGroupFilter.shouldRead(
                parquetSchema, rowGroup, reader.getDictionaryReader(rowGroup)));
        break;
    }

    // when expression matches rowGroup, should read the row group no matter what kind of encoding
    // format
    Geometry intersectPoint = TypeUtil.GeometryUtils.wkt2geometry("POINT (0 0)");
    exp = Expressions.stIntersects(geometryName, intersectPoint);
    metricRowGroupFilter = new ParquetMetricsRowGroupFilter(SCHEMA, exp);
    dictionaryRowGroupFilter = new ParquetDictionaryRowGroupFilter(SCHEMA, exp);
    Assert.assertTrue(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));
    Assert.assertTrue(
        dictionaryRowGroupFilter.shouldRead(
            parquetSchema, rowGroup, reader.getDictionaryReader(rowGroup)));

    // test parquet metric filter when the metric and the query window are just in touch
    if (!encoding.equals("wkb")) {
      Envelope envelope = new Envelope(N, N + 1, N, N + 1);
      GeometryFactory gf = new GeometryFactory();
      exp = Expressions.stIntersects(geometryName, gf.toGeometry(envelope));
      metricRowGroupFilter = new ParquetMetricsRowGroupFilter(SCHEMA, exp);
      Assert.assertTrue(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));

      exp = Expressions.stWithin(geometryName, gf.toGeometry(envelope));
      metricRowGroupFilter = new ParquetMetricsRowGroupFilter(SCHEMA, exp);
      Assert.assertFalse(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));

      exp = Expressions.stContains(geometryName, gf.toGeometry(envelope));
      metricRowGroupFilter = new ParquetMetricsRowGroupFilter(SCHEMA, exp);
      Assert.assertFalse(metricRowGroupFilter.shouldRead(parquetSchema, rowGroup));
    }
  }
}
