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
package org.apache.iceberg.spark.functions;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.iceberg.udt.GeometryUDT;
import org.apache.spark.sql.types.DataType;
import org.locationtech.jts.geom.Envelope;

public class GeomMinMax {
  public static final String MinX = "st_minx";
  public static final String MinY = "st_miny";
  public static final String MaxX = "st_maxx";
  public static final String MaxY = "st_maxy";

  public static UnboundFunction GeomMinXFunction = createAggFunction(MinX, new MinXFunc());
  public static UnboundFunction GeomMinYFunction = createAggFunction(MinY, new MinYFunc());
  public static UnboundFunction GeomMaxXFunction = createAggFunction(MaxX, new MaxXFunc());
  public static UnboundFunction GeomMaxYFunction = createAggFunction(MaxY, new MaxYFunc());

  static UnboundFunction createAggFunction(String name, AbstractGeomMinMaxAggFunction func) {
    return new UnaryUnboundFunction() {
      @Override
      protected BoundFunction doBind(DataType valueType) {
        if (valueType instanceof GeometryUDT) {
          return func;
        } else {
          throw new UnsupportedOperationException(
              "Expected value to be Geometry: " + valueType.catalogString());
        }
      }

      @Override
      public String description() {
        return name;
      }

      @Override
      public String name() {
        return name;
      }
    };
  }

  public static class MinXFunc extends AbstractGeomMinMaxAggFunction.MinGeomFunction {
    @Override
    public String name() {
      return MinX;
    }

    @Override
    Double dimensionVal(Envelope env) {
      return env.getMinX();
    }
  }

  public static class MinYFunc extends AbstractGeomMinMaxAggFunction.MinGeomFunction {
    @Override
    public String name() {
      return MinY;
    }

    @Override
    Double dimensionVal(Envelope env) {
      return env.getMinY();
    }
  }

  public static class MaxXFunc extends AbstractGeomMinMaxAggFunction.MaxGeomFunction {
    @Override
    public String name() {
      return MaxX;
    }

    @Override
    Double dimensionVal(Envelope env) {
      return env.getMaxX();
    }
  }

  public static class MaxYFunc extends AbstractGeomMinMaxAggFunction.MaxGeomFunction {
    @Override
    public String name() {
      return MaxY;
    }

    @Override
    Double dimensionVal(Envelope env) {
      return env.getMaxY();
    }
  }
}
