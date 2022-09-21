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

package org.apache.spark.sql.sedona


import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.LeafExpression
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.udt.GeometryUDT
import org.locationtech.jts.geom.Geometry

/**
 * Catalyst AST expression used during rule rewriting to extract geometry literal values
 * from Catalyst memory and keep a copy in JVM heap space for subsequent use in rule evaluation.
 */
case class GeometryLiteral(repr: Any, geom: Geometry) extends LeafExpression  with CodegenFallback {

  override def foldable: Boolean = true

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = repr

  override def dataType: DataType = GeometryUDT
}

