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

import java.io.IOException;
import java.time.OffsetDateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class TestTableWithGeometry {

  private static final TableIdentifier name = TableIdentifier.of("api", "geom_io_with_xz2");
  private static final Schema schema =
      new Schema(
          Types.NestedField.required(1, "level", Types.StringType.get()),
          Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
          Types.NestedField.required(3, "message", Types.StringType.get()),
          Types.NestedField.optional(4, "geom", Types.GeometryType.get()));
  private static final PartitionSpec spec =
      PartitionSpec.builderFor(schema)
          .hour("event_time")
          // .identity("level")
          // .truncate("level", 1)
          .xz2("geom", 12)
          .build();
  public static final Configuration conf = new Configuration();
  private static final String warehousePath = "/Users/duyalei/icerberg/warehouse";
  private static Table table;

  public static HadoopCatalog getCatalog() {
    HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);
    return catalog;
  }

  public static void creatTable() {
    HadoopCatalog catalog = getCatalog();
    System.out.println(catalog);
    if (!catalog.tableExists(name)) {
      System.out.printf("creating table %s\n", name);
      Table table = catalog.createTable(name, schema, spec);
    } else {
      System.out.printf("table %s already exists, no need to create\n", name);
    }
  }

  public static Table getTable() {
    HadoopCatalog catalog = getCatalog();
    return catalog.loadTable(name);
  }

  public static void descTable() {
    Table table = getTable();
    System.out.println("schema: ");
    System.out.println(table.schema());

    System.out.println("spec: ");
    System.out.println(table.spec());

    System.out.println("spec.fields: ");
    System.out.println(table.spec().fields());

    System.out.println("properties: ");
    System.out.println(table.properties());

    System.out.println("location: ");
    System.out.println(table.location());
  }

  public static void scanTable() {
    Table table = getTable();
    TableScan scan =
        table
            .newScan()
            // .filter(Expressions.greaterThanOrEqual("event_time",
            // "2022-08-09T00:00:00.000-06:00"))
            // .filter(Expressions.lessThan("event_time", "2022-08-10T00:00:00.000-06:00"))
            .filter(Expressions.equal("level", "info"));
    Iterable<CombinedScanTask> tasks = scan.planTasks();
    for (CombinedScanTask task : tasks) {
      DataFile dataFile = task.files().iterator().next().file();
      System.out.println(dataFile);
    }
  }

  public static void appendFile() {
    Table table = getTable();
    String path = warehousePath + "/api/test_geom.parquet";
    InputFile inputFile = HadoopInputFile.fromPath(new Path(path), conf);
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(spec)
                .withFormat(FileFormat.PARQUET)
                .withInputFile(inputFile)
                .withMetrics(ParquetUtil.fileMetrics(inputFile, MetricsConfig.getDefault()))
                .withFileSizeInBytes(inputFile.getLength())
                // .withPartition()
                // .withPartitionPath("event_time_hour=2022080900/level=info")
                .withPartitionPath("event_time_hour=2022090100/geom_xz2_12=123")
                .build())
        .commit();
  }

  public static void readTable() throws IOException {
    Table table = getTable();
    Geometry guangzhou =
        TypeUtil.GeometryUtils.wkt2geometry(
            "POLYGON ((113.582738 23.208284, 113.584522 23.207674, 113.583289 23.203502, 113.582738 23.208284))");
    String worldWkt = "POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))";
    String partialWkt = "POLYGON ((10 10, 15 10, 15 90, 10 90, 10 10))";
    Geometry world = TypeUtil.GeometryUtils.wkt2geometry(worldWkt);
    IcebergGenerics.ScanBuilder scanBuilder =
        IcebergGenerics.read(table).where(Expressions.stIn("geom", world));
    try (CloseableIterable<Record> result = scanBuilder.build()) {
      int i = 0;
      for (Record r : result) {
        System.out.println(i++);
        System.out.println(r);
        System.out.printf(
            "geom: %s, class: %s\n",
            r.getField("geom"), r.getField("geom").getClass().getCanonicalName());
      }
    }
  }

  public static GeometryFactory ff = new GeometryFactory();

  public static void writeToTable() throws IOException {
    OffsetDateTime now = OffsetDateTime.now();
    int ts = (int) now.toEpochSecond();
    StructLike partition = TestHelpers.Row.of(ts / 3600);
    Table table = getTable();
    String dataFilePath = String.format(warehousePath + "/api/geom_io/data/%d.parquet", ts);
    // OutputFile outputFile = table.io().newOutputFile(dataFilePath);
    // DataWriter<Object> writer = Parquet.writeData(outputFile)
    //     .forTable(table)
    //     .withPartition(partition)
    //     .createWriterFunc(
    //         type -> GenericParquetWriter.buildWriter(table.schema(), type, table.properties()))
    //     .build();
    // for (long k = 0; k < 20L; k++) {
    //   Record record = GenericRecord.create(table.schema());
    //   record.set(0, String.format("level-%d", k));
    //   record.set(1, now);
    //   record.set(2, String.format("message-%d", k));
    //   Point pt = ff.createPoint(new Coordinate(k, 10 + k));
    //   if (k < 10) {
    //     // write geometry object
    //     record.set(3, pt);
    //   } else {
    //     // write WKB
    //     ByteBuffer geomBytes = TypeUtil.GeometryUtils.geometry2byteBuffer(pt);
    //     record.set(3, geomBytes);
    //   }
    //   writer.write(record);
    // }
    // writer.close();
    // DataFile dataFile = writer.toDataFile();
    // table.newAppend().appendFile(dataFile).commit();
  }

  public static void updateTableSchema() {
    // done
    Table table = getTable();
    table.updateSchema().addColumn("geom", Types.GeometryType.get()).commit();
    descTable();
  }

  public static void deleteRows() {
    Table table = getTable();
    DeleteFiles deleteFiles =
        table.newDelete().deleteFromRowFilter(Expressions.equal("level", "error"));
    // table.newRowDelta().addDeletes(deleteFiles);
    // table.newOverwrite().
  }

  public static void main(String[] args) throws IOException {
    creatTable();
    descTable();
    // appendFile();
    deleteRows();
    // scanTable();
    // writeToTable();
    readTable();
  }
}
