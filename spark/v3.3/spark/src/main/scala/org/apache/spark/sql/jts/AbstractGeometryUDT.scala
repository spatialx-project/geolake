/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.spark.sql.jts

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.Geometry
import scala.reflect._

/**
 * Base class for all JTS UDTs, which get encoded in Catalyst as WKB blobs.
 * @param simpleString short name, like "point"
 * @tparam T Concrete JTS type represented by this UDT
 */
abstract class AbstractGeometryUDT[T >: Null <: Geometry: ClassTag](override val simpleString: String)
  extends UserDefinedType[T] {

  override def pyUDT: String = s"iceberg_pyspark.types.${getClass.getSimpleName}"

  override def serialize(obj: T): InternalRow = {
    new GenericInternalRow(Array[Any](WKBUtils.write(obj)))
  }

  override def sqlType: DataType = StructType(Seq(
    StructField("wkb", DataTypes.BinaryType)
  ))

  override def userClass: Class[T] = classTag[T].runtimeClass.asInstanceOf[Class[T]]

  override def deserialize(datum: Any): T = {
    val ir = datum.asInstanceOf[InternalRow]
    WKBUtils.read(ir.getBinary(0)).asInstanceOf[T]
  }
}
