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
package org.apache.iceberg.transforms;

import java.nio.ByteBuffer;
import org.apache.iceberg.types.TypeUtil;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class TestXZIndex {
  @Test
  public void testIndex() {
    String wkt = "POLYGON ((0 0, 0 -1, 7.5 -1, 7.5 0, 0 0))";
    ByteBuffer wkb = TypeUtil.GeometryUtils.wkt2byteBuffer(wkt);
    Geometry geom = TypeUtil.GeometryUtils.byteBuffer2geometry(wkb);

    int resolution = 12;
    Transform<ByteBuffer, Long> xzBuffer = Transforms.xz2(resolution);
    Transform<Geometry, Long> xzGeom = Transforms.xz2(resolution);

    Long index = 9317037L;
    Assert.assertEquals(index, xzBuffer.apply(wkb));
    Assert.assertEquals(index, xzGeom.apply(geom));
  }
}
