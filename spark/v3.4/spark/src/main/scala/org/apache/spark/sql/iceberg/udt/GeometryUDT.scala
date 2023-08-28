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

import org.apache.spark.sql.types._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.locationtech.jts.geom.Geometry

class GeometryUDT extends UserDefinedType[Geometry] {
  override def sqlType: DataType = BinaryType

  // TODO: we deliberately leave this unchanged, so that the GeometryUDT will be
  //  compatible with pyspark binding of sedona. We'll implement our own pyspark UDT
  //  binding when optimizing serde of GeometryUDT.
  override def pyUDT: String = "sedona.sql.types.GeometryType"

  override def userClass: Class[Geometry] = classOf[Geometry]

  override def serialize(obj: Geometry): Array[Byte] =
    GeometrySerializer.serialize(obj)

  override def deserialize(datum: Any): Geometry = {
    GeometrySerializer.deserialize(datum)
  }

  override private[sql] def jsonValue: JValue = {
    super.jsonValue mapField {
      case ("class", _) => "class" -> this.getClass.getName.stripSuffix("$")
      case other: Any => other
    }
  }

  override def equals(other: Any): Boolean = other match {
    case _: UserDefinedType[_] => other.isInstanceOf[GeometryUDT]
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}

case object GeometryUDT extends GeometryUDT with scala.Serializable
