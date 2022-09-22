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
package org.apache.iceberg.expressions;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNaN;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNaN;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.predicate;
import static org.apache.iceberg.expressions.Expressions.stContains;
import static org.apache.iceberg.expressions.Expressions.stIntersects;
import static org.apache.iceberg.expressions.Expressions.stWithin;
import static org.apache.iceberg.expressions.Expressions.startsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;

public class TestEvaluator {
  private static final StructType STRUCT =
      StructType.of(
          required(13, "x", Types.IntegerType.get()),
          required(14, "y", Types.DoubleType.get()),
          optional(15, "z", Types.IntegerType.get()),
          optional(
              16,
              "s1",
              Types.StructType.of(
                  Types.NestedField.required(
                      17,
                      "s2",
                      Types.StructType.of(
                          Types.NestedField.required(
                              18,
                              "s3",
                              Types.StructType.of(
                                  Types.NestedField.required(
                                      19,
                                      "s4",
                                      Types.StructType.of(
                                          Types.NestedField.required(
                                              20, "i", Types.IntegerType.get()))))))))),
          optional(
              21,
              "s5",
              Types.StructType.of(
                  Types.NestedField.required(
                      22,
                      "s6",
                      Types.StructType.of(
                          Types.NestedField.required(23, "f", Types.FloatType.get()))))));
  private static final StructType GEOMETRY_STRUCT =
      StructType.of(required(1, "geom", Types.GeometryType.get()));
  private static final String WORLD_WKT =
      "POLYGON ((-180 -90, -180 90, 180 90, 180 -90, -180 -90))";
  private static final Geometry WORLD_GEOM = TypeUtil.GeometryUtils.wkt2geometry(WORLD_WKT);

  @Test
  public void testLessThan() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThan("x", 7));
    Assert.assertFalse("7 < 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null, null)));
    Assert.assertTrue("6 < 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, lessThan("s1.s2.s3.s4.i", 7));
    Assert.assertFalse(
        "7 < 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "6 < 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));
  }

  @Test
  public void testLessThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, lessThanOrEqual("x", 7));
    Assert.assertTrue("7 <= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 <= 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertFalse("8 <= 7 => false", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, lessThanOrEqual("s1.s2.s3.s4.i", 7));
    Assert.assertTrue(
        "7 <= 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));

    Assert.assertTrue(
        "6 <= 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));

    Assert.assertFalse(
        "8 <= 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testGreaterThan() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThan("x", 7));
    Assert.assertFalse("7 > 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 > 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 > 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, greaterThan("s1.s2.s3.s4.i", 7));
    Assert.assertFalse(
        "7 > 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "6 > 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));
    Assert.assertTrue(
        "8 > 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testGreaterThanOrEqual() {
    Evaluator evaluator = new Evaluator(STRUCT, greaterThanOrEqual("x", 7));
    Assert.assertTrue("7 >= 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 >= 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));
    Assert.assertTrue("8 >= 7 => true", evaluator.eval(TestHelpers.Row.of(8, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, greaterThanOrEqual("s1.s2.s3.s4.i", 7));
    Assert.assertTrue(
        "7 >= 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "6 >= 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));
    Assert.assertTrue(
        "8 >= 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testEqual() {
    Assert.assertEquals(1, equal("x", 5).literals().size());

    Evaluator evaluator = new Evaluator(STRUCT, equal("x", 7));
    Assert.assertTrue("7 == 7 => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("6 == 7 => false", evaluator.eval(TestHelpers.Row.of(6, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, equal("s1.s2.s3.s4.i", 7));
    Assert.assertTrue(
        "7 == 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "6 == 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));
  }

  @Test
  public void testNotEqual() {
    Assert.assertEquals(1, notEqual("x", 5).literals().size());

    Evaluator evaluator = new Evaluator(STRUCT, notEqual("x", 7));
    Assert.assertFalse("7 != 7 => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 != 7 => true", evaluator.eval(TestHelpers.Row.of(6, 8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, notEqual("s1.s2.s3.s4.i", 7));
    Assert.assertFalse(
        "7 != 7 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "6 != 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));
  }

  @Test
  public void testStartsWith() {
    StructType struct = StructType.of(required(24, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, startsWith("s", "abc"));
    Assert.assertTrue(
        "abc startsWith abc should be true", evaluator.eval(TestHelpers.Row.of("abc")));
    Assert.assertFalse(
        "xabc startsWith abc should be false", evaluator.eval(TestHelpers.Row.of("xabc")));
    Assert.assertFalse(
        "Abc startsWith abc should be false", evaluator.eval(TestHelpers.Row.of("Abc")));
    Assert.assertFalse("a startsWith abc should be false", evaluator.eval(TestHelpers.Row.of("a")));
    Assert.assertTrue(
        "abcd startsWith abc should be true", evaluator.eval(TestHelpers.Row.of("abcd")));
    Assert.assertFalse(
        "null startsWith abc should be false", evaluator.eval(TestHelpers.Row.of((String) null)));
  }

  @Test
  public void testNotStartsWith() {
    StructType struct = StructType.of(required(24, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, notStartsWith("s", "abc"));
    Assert.assertFalse(
        "abc notStartsWith abc should be false", evaluator.eval(TestHelpers.Row.of("abc")));
    Assert.assertTrue(
        "xabc notStartsWith abc should be true", evaluator.eval(TestHelpers.Row.of("xabc")));
    Assert.assertTrue(
        "Abc notStartsWith abc should be true", evaluator.eval(TestHelpers.Row.of("Abc")));
    Assert.assertTrue(
        "a notStartsWith abc should be true", evaluator.eval(TestHelpers.Row.of("a")));
    Assert.assertFalse(
        "abcde notStartsWith abc should be false", evaluator.eval(TestHelpers.Row.of("abcde")));
    Assert.assertTrue(
        "Abcde notStartsWith abc should be true", evaluator.eval(TestHelpers.Row.of("Abcde")));
  }

  @Test
  public void testAlwaysTrue() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysTrue());
    Assert.assertTrue("always true", evaluator.eval(TestHelpers.Row.of()));
  }

  @Test
  public void testAlwaysFalse() {
    Evaluator evaluator = new Evaluator(STRUCT, alwaysFalse());
    Assert.assertFalse("always false", evaluator.eval(TestHelpers.Row.of()));
  }

  @Test
  public void testIsNull() {
    Evaluator evaluator = new Evaluator(STRUCT, isNull("z"));
    Assert.assertTrue("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertFalse("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));

    Evaluator structEvaluator = new Evaluator(STRUCT, isNull("s1.s2.s3.s4.i"));
    Assert.assertFalse(
        "3 is not null",
        structEvaluator.eval(
            TestHelpers.Row.of(
                1,
                2,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(3)))))));
  }

  @Test
  public void testNotNull() {
    Evaluator evaluator = new Evaluator(STRUCT, notNull("z"));
    Assert.assertFalse("null is null", evaluator.eval(TestHelpers.Row.of(1, 2, null)));
    Assert.assertTrue("3 is not null", evaluator.eval(TestHelpers.Row.of(1, 2, 3)));

    Evaluator structEvaluator = new Evaluator(STRUCT, notNull("s1.s2.s3.s4.i"));
    Assert.assertTrue(
        "3 is not null",
        structEvaluator.eval(
            TestHelpers.Row.of(
                1,
                2,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(3)))))));
  }

  @Test
  public void testIsNan() {
    Evaluator evaluator = new Evaluator(STRUCT, isNaN("y"));
    Assert.assertTrue("NaN is NaN", evaluator.eval(TestHelpers.Row.of(1, Double.NaN, 3)));
    Assert.assertFalse("2 is not NaN", evaluator.eval(TestHelpers.Row.of(1, 2.0, 3)));

    Evaluator structEvaluator = new Evaluator(STRUCT, isNaN("s5.s6.f"));
    Assert.assertTrue(
        "NaN is NaN",
        structEvaluator.eval(
            TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(Float.NaN)))));
    Assert.assertFalse(
        "4F is not NaN",
        structEvaluator.eval(
            TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(4F)))));
  }

  @Test
  public void testNotNaN() {
    Evaluator evaluator = new Evaluator(STRUCT, notNaN("y"));
    Assert.assertFalse("NaN is NaN", evaluator.eval(TestHelpers.Row.of(1, Double.NaN, 3)));
    Assert.assertTrue("2 is not NaN", evaluator.eval(TestHelpers.Row.of(1, 2.0, 3)));

    Evaluator structEvaluator = new Evaluator(STRUCT, notNaN("s5.s6.f"));
    Assert.assertFalse(
        "NaN is NaN",
        structEvaluator.eval(
            TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(Float.NaN)))));
    Assert.assertTrue(
        "4F is not NaN",
        structEvaluator.eval(
            TestHelpers.Row.of(1, 2, 3, null, TestHelpers.Row.of(TestHelpers.Row.of(4F)))));
  }

  @Test
  public void testAnd() {
    Evaluator evaluator = new Evaluator(STRUCT, and(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertFalse("8, 3 => false", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertFalse("7, null => false", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));

    Evaluator structEvaluator =
        new Evaluator(STRUCT, and(equal("s1.s2.s3.s4.i", 7), notNull("s1.s2.s3.s4.i")));

    Assert.assertTrue(
        "7, 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                0,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "8, 8 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                8,
                0,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));

    Assert.assertFalse(
        "7, null => false", structEvaluator.eval(TestHelpers.Row.of(7, 0, null, null)));

    Assert.assertFalse(
        "8, notnull => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                8,
                0,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testOr() {
    Evaluator evaluator = new Evaluator(STRUCT, or(equal("x", 7), notNull("z")));
    Assert.assertTrue("7, 3 => true", evaluator.eval(TestHelpers.Row.of(7, 0, 3)));
    Assert.assertTrue("8, 3 => true", evaluator.eval(TestHelpers.Row.of(8, 0, 3)));
    Assert.assertTrue("7, null => true", evaluator.eval(TestHelpers.Row.of(7, 0, null)));
    Assert.assertFalse("8, null => false", evaluator.eval(TestHelpers.Row.of(8, 0, null)));

    Evaluator structEvaluator =
        new Evaluator(STRUCT, or(equal("s1.s2.s3.s4.i", 7), notNull("s1.s2.s3.s4.i")));

    Assert.assertTrue(
        "7, 7 => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                0,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "8, 8 => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                8,
                0,
                3,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));

    Assert.assertTrue(
        "7, notnull => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                0,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
  }

  @Test
  public void testNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("x", 7)));
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)));
    Assert.assertFalse(
        "not(7 == 7) => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                null,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "not(8 == 7) => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                8,
                null,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testCaseInsensitiveNot() {
    Evaluator evaluator = new Evaluator(STRUCT, not(equal("X", 7)), false);
    Assert.assertFalse("not(7 == 7) => false", evaluator.eval(TestHelpers.Row.of(7)));
    Assert.assertTrue("not(8 == 7) => false", evaluator.eval(TestHelpers.Row.of(8)));

    Evaluator structEvaluator = new Evaluator(STRUCT, not(equal("s1.s2.s3.s4.i", 7)), false);
    Assert.assertFalse(
        "not(7 == 7) => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                null,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "not(8 == 7) => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                8,
                null,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(8)))))));
  }

  @Test
  public void testCaseSensitiveNot() {
    Assertions.assertThatThrownBy(() -> new Evaluator(STRUCT, not(equal("X", 7)), true))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'X' in struct");
  }

  @Test
  public void testCharSeqValue() {
    StructType struct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator evaluator = new Evaluator(struct, equal("s", "abc"));
    Assert.assertTrue(
        "string(abc) == utf8(abc) => true", evaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertFalse(
        "string(abc) == utf8(abcd) => false", evaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }

  @Test
  public void testWithin() {
    Assert.assertEquals(3, in("s", 7, 8, 9).literals().size());
    Assert.assertEquals(3, in("s", 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, in("s", "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, in("s").literals().size());
    Assert.assertEquals(1, in("s", 5).literals().size());
    Assert.assertEquals(2, in("s", 5, 5).literals().size());
    Assert.assertEquals(2, in("s", Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, in("s", Collections.emptyList()).literals().size());

    Evaluator evaluator = new Evaluator(STRUCT, in("x", 7, 8, Long.MAX_VALUE));
    Assert.assertTrue("7 in [7, 8] => true", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertFalse("9 in [7, 8]  => false", evaluator.eval(TestHelpers.Row.of(9, 8, null)));

    Evaluator intSetEvaluator =
        new Evaluator(STRUCT, in("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE));
    Assert.assertTrue(
        "Integer.MAX_VALUE in [Integer.MAX_VALUE] => true",
        intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)));
    Assert.assertFalse(
        "6 in [Integer.MAX_VALUE]  => false",
        intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Evaluator integerEvaluator = new Evaluator(STRUCT, in("y", 7, 8, 9.1));
    Assert.assertTrue(
        "7.0 in [7, 8, 9.1] => true", integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)));
    Assert.assertTrue(
        "9.1 in [7, 8, 9.1] => true", integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)));
    Assert.assertFalse(
        "6.8 in [7, 8, 9.1]  => false", integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, in("s1.s2.s3.s4.i", 7, 8, 9));
    Assert.assertTrue(
        "7 in [7, 8, 9] => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertFalse(
        "6 in [7, 8, 9]  => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, in("s", "abc", "abd", "abc"));
    Assert.assertTrue(
        "utf8(abc) in [string(abc), string(abd)] => true",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertFalse(
        "utf8(abcd) in [string(abc), string(abd)] => false",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }

  @Test
  public void testInExceptions() {
    Assertions.assertThatThrownBy(() -> in("x", (Literal) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create expression literal from null");

    Assertions.assertThatThrownBy(() -> in("x", (Collection<?>) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Values cannot be null for IN predicate.");

    Assertions.assertThatThrownBy(() -> in("x", 5, 6).literal())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("IN predicate cannot return a literal");

    Assertions.assertThatThrownBy(() -> in("x", 1, 2, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create expression literal from null");

    Assertions.assertThatThrownBy(() -> new Evaluator(STRUCT, in("x", 7, 8, 9.1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type int");

    Assertions.assertThatThrownBy(() -> predicate(Expression.Operation.IN, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create IN predicate without a value");

    Assertions.assertThatThrownBy(
            () -> new Evaluator(STRUCT, predicate(Expression.Operation.IN, "x", 5.1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type int");
  }

  @Test
  public void testNotIn() {
    Assert.assertEquals(3, notIn("s", 7, 8, 9).literals().size());
    Assert.assertEquals(3, notIn("s", 7, 8.1, Long.MAX_VALUE).literals().size());
    Assert.assertEquals(3, notIn("s", "abc", "abd", "abc").literals().size());
    Assert.assertEquals(0, notIn("s").literals().size());
    Assert.assertEquals(1, notIn("s", 5).literals().size());
    Assert.assertEquals(2, notIn("s", 5, 5).literals().size());
    Assert.assertEquals(2, notIn("s", Arrays.asList(5, 5)).literals().size());
    Assert.assertEquals(0, notIn("s", Collections.emptyList()).literals().size());

    Evaluator evaluator = new Evaluator(STRUCT, notIn("x", 7, 8, Long.MAX_VALUE));
    Assert.assertFalse("7 not in [7, 8] => false", evaluator.eval(TestHelpers.Row.of(7, 8, null)));
    Assert.assertTrue("6 not in [7, 8]  => true", evaluator.eval(TestHelpers.Row.of(9, 8, null)));

    Evaluator intSetEvaluator =
        new Evaluator(STRUCT, notIn("x", Long.MAX_VALUE, Integer.MAX_VALUE, Long.MIN_VALUE));
    Assert.assertFalse(
        "Integer.MAX_VALUE not_in [Integer.MAX_VALUE] => false",
        intSetEvaluator.eval(TestHelpers.Row.of(Integer.MAX_VALUE, 7.0, null)));
    Assert.assertTrue(
        "6 not_in [Integer.MAX_VALUE]  => true",
        intSetEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Evaluator integerEvaluator = new Evaluator(STRUCT, notIn("y", 7, 8, 9.1));
    Assert.assertFalse(
        "7.0 not in [7, 8, 9] => false", integerEvaluator.eval(TestHelpers.Row.of(0, 7.0, null)));
    Assert.assertFalse(
        "9.1 not in [7, 8, 9.1] => false", integerEvaluator.eval(TestHelpers.Row.of(7, 9.1, null)));
    Assert.assertTrue(
        "6.8 not in [7, 8, 9.1]  => true", integerEvaluator.eval(TestHelpers.Row.of(6, 6.8, null)));

    Evaluator structEvaluator = new Evaluator(STRUCT, notIn("s1.s2.s3.s4.i", 7, 8, 9));
    Assert.assertFalse(
        "7 not in [7, 8, 9] => false",
        structEvaluator.eval(
            TestHelpers.Row.of(
                7,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(7)))))));
    Assert.assertTrue(
        "6 not in [7, 8, 9]  => true",
        structEvaluator.eval(
            TestHelpers.Row.of(
                6,
                8,
                null,
                TestHelpers.Row.of(
                    TestHelpers.Row.of(TestHelpers.Row.of(TestHelpers.Row.of(6)))))));

    StructType charSeqStruct = StructType.of(required(34, "s", Types.StringType.get()));
    Evaluator charSeqEvaluator = new Evaluator(charSeqStruct, notIn("s", "abc", "abd", "abc"));
    Assert.assertFalse(
        "utf8(abc) not in [string(abc), string(abd)] => false",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abc"))));
    Assert.assertTrue(
        "utf8(abcd) not in [string(abc), string(abd)] => true",
        charSeqEvaluator.eval(TestHelpers.Row.of(new Utf8("abcd"))));
  }

  @Test
  public void testNotInExceptions() {
    Assertions.assertThatThrownBy(() -> notIn("x", (Literal) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create expression literal from null");

    Assertions.assertThatThrownBy(() -> notIn("x", (Collection<?>) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Values cannot be null for NOT_IN predicate.");

    Assertions.assertThatThrownBy(() -> notIn("x", 5, 6).literal())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("NOT_IN predicate cannot return a literal");

    Assertions.assertThatThrownBy(() -> notIn("x", 1, 2, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create expression literal from null");

    Assertions.assertThatThrownBy(() -> new Evaluator(STRUCT, notIn("x", 7, 8, 9.1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type int");

    Assertions.assertThatThrownBy(() -> predicate(Expression.Operation.NOT_IN, "x"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create NOT_IN predicate without a value");

    Assertions.assertThatThrownBy(
            () -> new Evaluator(STRUCT, predicate(Expression.Operation.NOT_IN, "x", 5.1)))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid value for conversion to type int");
  }

  @Test
  public void testStInAndStContain() {
    Random random = new Random();
    ByteBuffer worldBuffer = TypeUtil.GeometryUtils.wkt2byteBuffer(WORLD_WKT);
    String northHalfWorld = "POLYGON((-180 0, -180 90, 180 90, 180 0, -180 0))";
    ByteBuffer northHalfWorldBuffer = TypeUtil.GeometryUtils.wkt2byteBuffer(northHalfWorld);

    for (int i = 0; i < 50; i++) {
      double longitude = random.nextDouble() * 360 - 180;
      double latitude = random.nextDouble() * 180 - 90;
      String wkt = String.format("POINT(%.2f %.2f)", longitude, latitude);
      ByteBuffer pointBuffer = TypeUtil.GeometryUtils.wkt2byteBuffer(wkt);
      Geometry point = TypeUtil.GeometryUtils.byteBuffer2geometry(pointBuffer);

      Evaluator inEvaluator = new Evaluator(GEOMETRY_STRUCT, stWithin("geom", WORLD_GEOM));
      Assert.assertTrue(
          "every single point in world => true", inEvaluator.eval(TestHelpers.Row.of(pointBuffer)));

      Evaluator containEvaluator = new Evaluator(GEOMETRY_STRUCT, stContains("geom", point));
      Assert.assertTrue(
          "world contains every single point=> true",
          containEvaluator.eval(TestHelpers.Row.of(worldBuffer)));

      Evaluator intersectEvaluator = new Evaluator(GEOMETRY_STRUCT, stIntersects("geom", point));
      Assert.assertTrue(
          "world is overlapped with every single point => true",
          intersectEvaluator.eval(TestHelpers.Row.of(worldBuffer)));

      Assert.assertEquals(
          wkt + " is in the northern Hemisphere => " + (latitude > 0),
          latitude > 0,
          intersectEvaluator.eval(TestHelpers.Row.of(northHalfWorldBuffer)));
    }
  }
}
