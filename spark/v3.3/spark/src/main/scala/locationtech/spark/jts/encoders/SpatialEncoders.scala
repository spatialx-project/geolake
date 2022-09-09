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

package locationtech.spark.jts.encoders

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.locationtech.jts.geom._

/** Encoders are Spark SQL's mechanism for converting a JVM type into a Catalyst representation.
 * They are fetched from implicit scope whenever types move beween RDDs and Datasets. Because each
 * of the types supported below has a corresponding UDT, we are able to use a standard Spark Encoder
 * to construct these implicits. */
trait SpatialEncoders {
  implicit def jtsGeometryEncoder: Encoder[Geometry] = ExpressionEncoder()
  implicit def jtsPointEncoder: Encoder[Point] = ExpressionEncoder()
  implicit def jtsLineStringEncoder: Encoder[LineString] = ExpressionEncoder()
  implicit def jtsPolygonEncoder: Encoder[Polygon] = ExpressionEncoder()
  implicit def jtsMultiPointEncoder: Encoder[MultiPoint] = ExpressionEncoder()
  implicit def jtsMultiLineStringEncoder: Encoder[MultiLineString] = ExpressionEncoder()
  implicit def jtsMultiPolygonEncoder: Encoder[MultiPolygon] = ExpressionEncoder()
  implicit def jtsGeometryCollectionEncoder: Encoder[GeometryCollection] = ExpressionEncoder()
}
