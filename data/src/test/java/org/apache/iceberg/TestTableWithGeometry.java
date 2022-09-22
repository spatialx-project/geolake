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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTableWithGeometry {

  private static final Logger LOG = LoggerFactory.getLogger(TestTableWithGeometry.class);
  private static final TableIdentifier name = TableIdentifier.of("api", "geom_io_with_xz2");
  private static final Schema schema =
      new Schema(
          Types.NestedField.required(1, "level", Types.StringType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "message", Types.StringType.get()),
          Types.NestedField.optional(4, "geom", Types.GeometryType.get()));
  private static final PartitionSpec spec =
      PartitionSpec.builderFor(schema).hour("event_time").build();
  public static final Configuration conf = new Configuration();
  private static File warehouse = null;
  private static String warehousePath = "";
  private static HadoopCatalog catalog = null;
  private static Table table = null;
  private static final long N = 20L;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
    warehousePath = (new Path(warehouse.getAbsolutePath())).toString();
    catalog = new HadoopCatalog(conf, warehousePath);
    if (!catalog.tableExists(name)) {
      table = catalog.createTable(name, schema, spec);
      descTable();
      writeToTable();
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

  private static void writeToTable() throws IOException {
    OffsetDateTime now = OffsetDateTime.now();
    int ts = (int) now.toEpochSecond();
    StructLike partition = TestHelpers.Row.of(ts / 3600);
    String dataFilePath = String.format(warehousePath + "/%d.parquet", ts);
    OutputFile outputFile = table.io().newOutputFile(dataFilePath);
    DataWriter<Object> writer =
        Parquet.writeData(outputFile)
            .forTable(table)
            .withPartition(partition)
            .createWriterFunc(
                type -> GenericParquetWriter.buildWriter(table.schema(), type, table.properties()))
            .build();
    GeometryFactory ff = new GeometryFactory();
    for (long k = 0; k < N; k++) {
      Record record = GenericRecord.create(table.schema());
      record.set(0, String.format("level-%d", k));
      record.set(1, now);
      record.set(2, String.format("message-%d", k));
      Point pt = ff.createPoint(new Coordinate(k, 10 + k));
      if (k < 10) {
        // write geometry object
        record.set(3, pt);
      } else {
        // write WKB
        ByteBuffer geomBytes = TypeUtil.GeometryUtils.geometry2byteBuffer(pt);
        record.set(3, geomBytes);
      }
      writer.write(record);
    }
    writer.close();
    DataFile dataFile = writer.toDataFile();
    table.newAppend().appendFile(dataFile).commit();
  }

  @Test
  public void testGeometryIo() throws IOException {
    List<Record> records = readTable(Expressions.alwaysTrue());
    Assert.assertEquals("Number of records should be equal", N, records.size());
    for (int i = 0; i < N; i++) {
      Record record = records.get(i);
      Assert.assertEquals(
          "level should be equal", String.format("level-%d", i), record.getField("level"));
      Assert.assertEquals(
          "message should be equal", String.format("message-%d", i), record.getField("message"));
      Assert.assertEquals(
          "geom should be equal",
          String.format("POINT (%d %d)", i, i + 10),
          record.getField("geom").toString());
    }

    records = readTable(Expressions.alwaysFalse());
    Assert.assertEquals("no result returned", 0, records.size());
  }
}
