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
package org.apache.iceberg.parquet;

import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBConstants;

public class GeoParquetGeometryBuilder {
  public static class NestedListGeometryBuilder {

    private final GeometryFactory ff;
    private final int type;
    private final List<Coordinate> coordinates;
    private final List<Integer> coordRanges;
    private final List<Integer> lineRanges;
    private final List<Integer> geomRanges;
    private final List<Integer> geomTypes;

    public NestedListGeometryBuilder(
        GeometryFactory ff,
        int type,
        List<Coordinate> coordinates,
        List<Integer> coordRanges,
        List<Integer> lineRanges,
        List<Integer> geomRanges,
        List<Integer> geomTypes) {
      this.ff = ff;
      this.type = type;
      this.coordinates = coordinates;
      this.coordRanges = coordRanges;
      this.lineRanges = lineRanges;
      this.geomRanges = geomRanges;
      this.geomTypes = geomTypes;
    }

    public Geometry build() {
      switch (type) {
        case WKBConstants.wkbPoint:
          return buildPoint(0, coordinates != null ? coordinates.size() : 0);
        case WKBConstants.wkbMultiPoint:
          return buildMultiPoint(0, coordinates != null ? coordinates.size() : 0);
        case WKBConstants.wkbLineString:
          return buildLineString(0, coordinates != null ? coordinates.size() : 0);
        case WKBConstants.wkbMultiLineString:
          return buildMultiLineString(0, coordRanges != null ? coordRanges.size() : 0);
        case WKBConstants.wkbPolygon:
          return buildPolygon(0, coordRanges != null ? coordRanges.size() : 0);
        case WKBConstants.wkbMultiPolygon:
          return buildMultiPolygon(0, lineRanges != null ? lineRanges.size() : 0);
        case WKBConstants.wkbGeometryCollection:
          return buildGeometryCollection(0, geomRanges != null ? geomRanges.size() : 0);
        default:
          throw new RuntimeException("Unknown WKB geometry type ID: " + type);
      }
    }

    private Point buildPoint(int begin, int end) {
      if (begin < end) {
        Coordinate coordinate = coordinates.get(begin);
        return ff.createPoint(coordinate);
      } else {
        return ff.createPoint();
      }
    }

    private MultiPoint buildMultiPoint(int begin, int end) {
      if (begin < end) {
        Point[] points = new Point[end - begin];
        for (int k = begin, i = 0; k < end; k++, i++) {
          Coordinate coordinate = coordinates.get(k);
          points[i] = ff.createPoint(coordinate);
        }
        return ff.createMultiPoint(points);
      } else {
        return ff.createMultiPoint();
      }
    }

    private LineString buildLineString(int begin, int end) {
      if (begin < end) {
        Coordinate[] nodeCoordinates = new Coordinate[end - begin];
        for (int k = begin, i = 0; k < end; k++, i++) {
          nodeCoordinates[i] = this.coordinates.get(k);
        }
        return ff.createLineString(nodeCoordinates);
      } else {
        return ff.createLineString();
      }
    }

    private LinearRing buildLinearRing(int begin, int end) {
      if (begin < end) {
        Coordinate[] nodeCoordinates = new Coordinate[end - begin];
        for (int k = begin, i = 0; k < end; k++, i++) {
          nodeCoordinates[i] = this.coordinates.get(k);
        }
        return ff.createLinearRing(nodeCoordinates);
      } else {
        return ff.createLinearRing();
      }
    }

    private MultiLineString buildMultiLineString(int begin, int end) {
      if (begin < end) {
        LineString[] lineStrings = new LineString[end - begin];
        for (int k = begin, i = 0; k < end; k++, i++) {
          lineStrings[i] = buildLineString(k > 0 ? coordRanges.get(k - 1) : 0, coordRanges.get(k));
        }
        return ff.createMultiLineString(lineStrings);
      } else {
        return ff.createMultiLineString();
      }
    }

    private Polygon buildPolygon(int begin, int end) {
      if (begin < end) {
        int numRings = end - begin;
        LinearRing shell =
            buildLinearRing(begin > 0 ? coordRanges.get(begin - 1) : 0, coordRanges.get(begin));
        if (numRings == 1) {
          return ff.createPolygon(shell);
        }
        LinearRing[] holes = new LinearRing[numRings - 1];
        for (int k = begin + 1, i = 0; i < numRings - 1; i++, k++) {
          holes[i] = buildLinearRing(coordRanges.get(k - 1), coordRanges.get(k));
        }
        return ff.createPolygon(shell, holes);
      } else {
        return ff.createPolygon();
      }
    }

    private MultiPolygon buildMultiPolygon(int begin, int end) {
      if (begin < end) {
        Polygon[] polygons = new Polygon[end - begin];
        for (int k = begin, i = 0; k < end; k++, i++) {
          polygons[i] = buildPolygon(k > 0 ? lineRanges.get(k - 1) : 0, lineRanges.get(k));
        }
        return ff.createMultiPolygon(polygons);
      } else {
        return ff.createMultiPolygon();
      }
    }

    private GeometryCollection buildGeometryCollection(int begin, int end) {
      if (begin >= end) {
        return ff.createGeometryCollection();
      }
      Geometry[] geoms = new Geometry[end - begin];
      for (int k = begin, i = 0; k < end; k++, i++) {
        int geomType = geomTypes.get(i);
        int lineRangeBegin = (k > 0 ? geomRanges.get(k - 1) : 0);
        int lineRangeEnd = geomRanges.get(k);
        int coordRangeBegin = (lineRangeBegin > 0 ? lineRanges.get(lineRangeBegin - 1) : 0);
        int coordRangeEnd = (lineRangeEnd > 0 ? lineRanges.get(lineRangeEnd - 1) : 0);
        int coordBegin = (coordRangeBegin > 0 ? coordRanges.get(coordRangeBegin - 1) : 0);
        int coordEnd = (coordRangeEnd > 0 ? coordRanges.get(coordRangeEnd - 1) : 0);
        switch (geomType) {
          case WKBConstants.wkbPoint:
            geoms[i] = buildPoint(coordBegin, coordEnd);
            break;
          case WKBConstants.wkbMultiPoint:
            geoms[i] = buildMultiPoint(coordBegin, coordEnd);
            break;
          case WKBConstants.wkbLineString:
            geoms[i] = buildLineString(coordBegin, coordEnd);
            break;
          case WKBConstants.wkbMultiLineString:
            geoms[i] = buildMultiLineString(coordRangeBegin, coordRangeEnd);
            break;
          case WKBConstants.wkbPolygon:
            geoms[i] = buildPolygon(coordRangeBegin, coordRangeEnd);
            break;
          case WKBConstants.wkbMultiPolygon:
            geoms[i] = buildMultiPolygon(lineRangeBegin, lineRangeEnd);
            break;
          case WKBConstants.wkbGeometryCollection:
            throw new UnsupportedOperationException("Nested GeometryCollection is not supported");
          default:
            throw new RuntimeException("Unknown WKB geometry type ID: " + geomType);
        }
      }
      return ff.createGeometryCollection(geoms);
    }
  }
}
