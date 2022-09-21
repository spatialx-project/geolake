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

import java.nio.ByteBuffer;
import org.apache.iceberg.types.TypeUtil;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

public class testPartition {
  @Test
  public void testSerde() {
    String s = "POINT(0 1)";
    org.locationtech.jts.geom.Geometry geometry = TypeUtil.GeometryUtils.wkt2geometry(s);
    WKBWriter wkbWriter = new WKBWriter(3, 2, true);
    byte[] write = wkbWriter.write(geometry);
    for (byte b : write) {
      System.out.print(b + ",");
    }
    System.out.println("byte[]: " + write);
    ByteBuffer wrap = ByteBuffer.wrap(write);
    System.out.println("bytebuffer: " + wrap);
    Geometry geometry1 = TypeUtil.GeometryUtils.byteBuffer2geometry(wrap);
    System.out.println("geometry1: " + geometry1);
  }
}
