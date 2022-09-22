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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.PARQUET_GEOMETRY_WRITE_ENCODING;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestTableWithGeometry {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableWithGeometry.class);
  private static final TableIdentifier tableName = TableIdentifier.of("api", "geom_io");
  private static final String geometryName = "geom";
  private static final Schema schema =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, geometryName, Types.GeometryType.get()),
          Types.NestedField.optional(3, "event_time", Types.TimestampType.withZone()));

  private static final PartitionSpec spec =
      PartitionSpec.builderFor(schema).hour("event_time").build();
  public static final Configuration conf = new Configuration();
  private static File warehouse = null;
  private static String warehousePath = "";
  private static HadoopCatalog catalog = null;
  private static Table table = null;
  private static final int N = 20;
  private final String encoding;
  private String dataFilePath;

  public TestTableWithGeometry(String encoding) {
    this.encoding = encoding;
  }

  @Parameterized.Parameters(name = "encoding = {0}")
  public static Object[] parameters() {
    return new Object[] {"wkb-bbox", "nested-list", "wkb"};
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
    warehousePath = (new Path(warehouse.getAbsolutePath())).toString();
    catalog = new HadoopCatalog(conf, warehousePath);
  }

  @AfterClass
  public static void dropWarehouse() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path path = new Path(warehouse.getAbsolutePath());
      FileSystem fs = path.getFileSystem(conf);
      Assert.assertTrue("Failed to delete " + warehousePath, fs.delete(path, true));
    }
  }

  private void initTable() throws IOException {
    if (!catalog.tableExists(tableName)) {
      Map<String, String> properties = Maps.newHashMap();
      properties.put(PARQUET_GEOMETRY_WRITE_ENCODING, encoding);
      table = catalog.createTable(tableName, schema, spec, properties);
      writeToTable();
      descTable();
    }
  }

  private void dropTable() {
    if (catalog.tableExists(tableName)) {
      catalog.dropTable(tableName);
    }
  }

  private static void descTable() {
    LOG.info("schema: " + table.schema());
    LOG.info("spec: " + table.spec());
    LOG.info("properties: " + table.properties());
    LOG.info("location: " + table.location());
  }

  private List<Record> readTable(Expression expression) throws IOException {
    IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table).where(expression);
    List<Record> data = Lists.newArrayList();
    try (CloseableIterable<Record> result = scanBuilder.build()) {
      for (Record r : result) {
        data.add(r);
      }
    }
    return data;
  }

  private void writeToTable() throws IOException {
    OffsetDateTime now = OffsetDateTime.now();
    int ts = (int) now.toEpochSecond();
    StructLike partition = TestHelpers.Row.of(ts / 3600);
    dataFilePath = String.format(warehousePath + "/%d.parquet", ts);
    OutputFile outputFile = table.io().newOutputFile(dataFilePath);
    DataWriter<Object> writer =
        Parquet.writeData(outputFile)
            .forTable(table)
            .withPartition(partition)
            .createWriterFunc(
                type -> GenericParquetWriter.buildWriter(table.schema(), type, table.properties()))
            .build();
    GeometryFactory ff = new GeometryFactory();
    for (int k = 0; k < N; k++) {
      Record record = GenericRecord.create(table.schema());
      record.set(0, k);
      Point pt = ff.createPoint(new Coordinate(k, k));
      if (k < 10) {
        // write geometry object
        record.set(1, pt);
      } else {
        // write WKB
        ByteBuffer geomBytes = TypeUtil.GeometryUtils.geometry2byteBuffer(pt);
        record.set(1, geomBytes);
      }
      record.set(2, now);
      writer.write(record);
    }
    writer.close();
    DataFile dataFile = writer.toDataFile();
    LOG.info("append data file: " + dataFilePath);
    table.newAppend().appendFile(dataFile).commit();
  }

  @Test
  public void testGeometryIo() throws IOException {
    initTable();

    // test all records
    List<Record> records = readTable(Expressions.alwaysTrue());
    Assert.assertEquals("Number of records should be equal", N, records.size());
    for (int i = 0; i < N; i++) {
      Record record = records.get(i);
      Assert.assertEquals("id should be equal", i, record.getField("id"));
      Assert.assertEquals(
          "geom should be equal",
          String.format("POINT (%d %d)", i, i),
          record.getField(geometryName).toString());
    }

    records = readTable(Expressions.alwaysFalse());
    Assert.assertEquals("no result returned", 0, records.size());

    // test reading with geometry filter
    Geometry geometry =
        TypeUtil.GeometryUtils.wkt2geometry("POLYGON ((0 0, 0 10, 10 10, 10 0, 0 0))");
    records = readTable(Expressions.stWithin(geometryName, geometry));
    Assert.assertEquals("returned size should be 9", 9, records.size());

    dropTable();
  }
}
