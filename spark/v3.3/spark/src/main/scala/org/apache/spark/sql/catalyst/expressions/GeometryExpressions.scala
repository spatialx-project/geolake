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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.iceberg.udt.GeometrySerializer
import org.apache.spark.sql.iceberg.udt.GeometryUDT
import org.apache.spark.sql.types.AbstractDataType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.WKTReader
import org.locationtech.jts.io.WKTWriter

abstract class IcebergGeometryPredicate extends BinaryExpression with Predicate with CodegenFallback
  with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT, GeometryUDT)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val leftGeometry = GeometrySerializer.deserialize(input1)
    val rightGeometry = GeometrySerializer.deserialize(input2)
    evalGeometry(leftGeometry, rightGeometry)
  }

  def evalGeometry(leftGeometry: Geometry, rightGeometry: Geometry): Any
}

case class IcebergSTCovers(left: Expression, right: Expression) extends IcebergGeometryPredicate {

  override def evalGeometry(leftGeometry: Geometry, rightGeometry: Geometry): Any = leftGeometry.covers(rightGeometry)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class IcebergSTCoveredBy(left: Expression, right: Expression) extends IcebergGeometryPredicate {

  override def evalGeometry(leftGeometry: Geometry, rightGeometry: Geometry): Any = leftGeometry.within(rightGeometry)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class IcebergSTIntersects(left: Expression, right: Expression) extends IcebergGeometryPredicate {
  override def evalGeometry(leftGeometry: Geometry, rightGeometry: Geometry): Any =
    leftGeometry.intersects(rightGeometry)

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(left = newLeft, right = newRight)
}

case class IcebergSTGeomFromText(child: Expression) extends UnaryExpression with CodegenFallback
  with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)

  override def dataType: DataType = GeometryUDT

  override protected def nullSafeEval(value: Any): Any = {
    val wkt = value.asInstanceOf[UTF8String].toString
    val wktReader = new WKTReader()
    val geom = wktReader.read(wkt)
    GeometrySerializer.serialize(geom)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

case class IcebergSTAsText(child: Expression) extends UnaryExpression with CodegenFallback
  with ExpectsInputTypes with NullIntolerant {

  override def inputTypes: Seq[AbstractDataType] = Seq(GeometryUDT)

  override def dataType: DataType = StringType

  override protected def nullSafeEval(value: Any): Any = {
    val geom = GeometrySerializer.deserialize(value)
    val wktWriter = new WKTWriter()
    UTF8String.fromString(wktWriter.write(geom))
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(child = newChild)
}

object GeometryExpressions {
  private val functions = Seq(
    (FunctionIdentifier("IcebergSTCovers"),
      new ExpressionInfo(classOf[IcebergSTCovers].getName, "IcebergSTCovers"),
      (children: Seq[Expression]) => IcebergSTCovers(children.head, children.last)),
    (FunctionIdentifier("IcebergSTIntersects"),
      new ExpressionInfo(classOf[IcebergSTIntersects].getName, "IcebergSTIntersects"),
      (children: Seq[Expression]) => IcebergSTIntersects(children.head, children.last)),
    (FunctionIdentifier("IcebergSTCoveredBy"),
      new ExpressionInfo(classOf[IcebergSTCoveredBy].getName, "IcebergSTCoveredBy"),
      (children: Seq[Expression]) => IcebergSTCoveredBy(children.head, children.last)),
    (FunctionIdentifier("IcebergSTGeomFromText"),
      new ExpressionInfo(classOf[IcebergSTGeomFromText].getName, "IcebergSTGeomFromText"),
      (children: Seq[Expression]) => IcebergSTGeomFromText(children.head)),
    (FunctionIdentifier("IcebergSTAsText"),
      new ExpressionInfo(classOf[IcebergSTAsText].getName, "IcebergSTAsText"),
      (children: Seq[Expression]) => IcebergSTAsText(children.head)))

  def registerFunctions(extensions: SparkSessionExtensions): Unit = {
    functions.foreach(extensions.injectFunction)
  }

  def registerFunctions(functionRegistry: FunctionRegistry): Unit = {
    functions.foreach {
      case (ident, expr, builder) => functionRegistry.registerFunction(ident, expr, builder)
    }
  }
}
