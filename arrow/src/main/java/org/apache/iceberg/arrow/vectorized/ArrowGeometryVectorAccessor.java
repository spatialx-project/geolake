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
package org.apache.iceberg.arrow.vectorized;

import java.util.List;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.parquet.GeoParquetGeometryBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class ArrowGeometryVectorAccessor {
  private ArrowGeometryVectorAccessor() {}

  public static class NestedListAccessor {
    private final StructVector vec;
    private final IntVector typeVec;
    private final ListVector xVec;
    private final ListVector yVec;
    private final ListVector zVec;
    private final ListVector mVec;
    private final ListVector coordRangeVec;
    private final ListVector lineRangeVec;
    private final ListVector geomRangeVec;
    private final ListVector geomTypeVec;
    private final GeometryFactory ff = new GeometryFactory();

    public NestedListAccessor(StructVector vector) {
      this.vec = vector;
      typeVec = vec.getChild("type", IntVector.class);
      xVec = vec.getChild("x", ListVector.class);
      yVec = vec.getChild("y", ListVector.class);
      zVec = vec.getChild("z", ListVector.class);
      mVec = vec.getChild("m", ListVector.class);
      coordRangeVec = vec.getChild("coordinate_ranges", ListVector.class);
      lineRangeVec = vec.getChild("line_ranges", ListVector.class);
      geomRangeVec = vec.getChild("geometry_ranges", ListVector.class);
      geomTypeVec = vec.getChild("geometry_types", ListVector.class);
    }

    @SuppressWarnings("unchecked")
    public Geometry getGeometry(int rowId) {
      // Caller should guarantee that vec[rowId] is not null.
      int type = typeVec.get(rowId);
      List<Double> xs = (List<Double>) xVec.getObject(rowId);
      List<Double> ys = (List<Double>) yVec.getObject(rowId);
      List<Double> zs = (List<Double>) zVec.getObject(rowId);
      List<Double> ms = (List<Double>) mVec.getObject(rowId);
      List<Integer> coordRanges = (List<Integer>) coordRangeVec.getObject(rowId);
      List<Integer> lineRanges = (List<Integer>) lineRangeVec.getObject(rowId);
      List<Integer> geomRanges = (List<Integer>) geomRangeVec.getObject(rowId);
      List<Integer> geomTypes = (List<Integer>) geomTypeVec.getObject(rowId);
      List<Coordinate> coordinates = assembleCoordinates(xs, ys, zs, ms);
      Geometry geometry =
          new GeoParquetGeometryBuilder.NestedListGeometryBuilder(
                  ff, type, coordinates, coordRanges, lineRanges, geomRanges, geomTypes)
              .build();
      return geometry;
    }

    private static List<Coordinate> assembleCoordinates(
        List<Double> xs, List<Double> ys, List<Double> zs, List<Double> ms) {
      List<Coordinate> coordinates = Lists.newArrayList();
      if (xs == null) {
        return coordinates;
      }
      boolean hasZ = zs != null;
      boolean hasM = ms != null;
      int coordIndex = 0;
      int numCoordinates = xs.size();
      for (int k = 0; k < numCoordinates; k++) {
        double coordX = xs.get(k);
        double coordY = ys.get(k);
        Coordinate coordinate;
        if (hasZ && hasM) {
          coordinate = new CoordinateXYZM(coordX, coordY, zs.get(coordIndex), ms.get(coordIndex));
        } else if (hasZ) {
          coordinate = new Coordinate(coordX, coordY, zs.get(coordIndex));
        } else if (hasM) {
          coordinate = new CoordinateXYM(coordX, coordY, ms.get(coordIndex));
        } else {
          coordinate = new CoordinateXY(coordX, coordY);
        }
        coordinates.add(coordinate);
      }
      return coordinates;
    }
  }
}
