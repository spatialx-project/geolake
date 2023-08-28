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
package org.apache.spark.sql.iceberg.udt

import org.apache.iceberg.geometry.serde
import org.locationtech.jts.geom.Geometry

/**
 * SerDe using the WKB reader and writer objects
 */
object GeometrySerializer {

  /**
   * Given a geometry returns array of bytes
   *
   * @param geometry JTS geometry
   * @return Array of bites represents this geometry
   */
  def serialize(geometry: Geometry): Array[Byte] = {
    serde.GeometrySerializer.serialize(geometry)
  }

  /**
   * Given ArrayData returns Geometry
   *
   * @param values ArrayData
   * @return JTS geometry
   */
  def deserialize(datum: Any): Geometry = {
    datum match {
      case values: Array[Byte] => deserialize(values)
    }
  }

  def deserialize(values: Array[Byte]): Geometry = {
    serde.GeometrySerializer.deserialize(values)
  }
}
