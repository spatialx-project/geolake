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
package org.apache.spark.sql.udt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKBReader
import org.locationtech.jts.io.WKBWriter

/**
 * SerDe using the WKB reader and writer objects
 */
object GeometrySerializer extends Serializer[Geometry] {

  /**
   * Given a geometry returns array of bytes
   *
   * @param geometry JTS geometry
   * @return Array of bites represents this geometry
   */
  def serialize(geometry: Geometry): Array[Byte] = {
    val writer = new WKBWriter(getDimension(geometry), 2, true)
    writer.write(geometry)
  }

  /**
   * Given ArrayData returns Geometry
   *
   * @param values ArrayData
   * @return JTS geometry
   */
  def deserialize(values: ArrayData): Geometry = {
    val reader = new WKBReader()
    reader.read(values.toByteArray())
  }

  private def getDimension(geometry: Geometry): Int = {
    if (geometry.getCoordinate != null && !geometry.getCoordinate.getZ.isNaN) {
      3
    } else {
      2
    }
  }

  override def write(kryo: Kryo, output: Output, geo: Geometry): Unit = {
    val bytes = serialize(geo)
    output.writeInt(bytes.length, true)
    output.write(bytes)
  }

  override def read(kryo: Kryo, input: Input, `type`: Class[Geometry]): Geometry = {
    val bytes = Array.ofDim[Byte](input.readInt(true))
    val reader = new WKBReader()
    reader.read(bytes)
  }
}
