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
package org.apache.iceberg.transforms;

import org.apache.iceberg.expressions.BoundRangePredicate;
import org.apache.iceberg.expressions.BoundTerm;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.geometry.IndexRangeSet;
import org.apache.iceberg.types.Types;

public class UnboundRangePredicate extends UnboundPredicate<Long> {
  private final IndexRangeSet rangeSet;

  UnboundRangePredicate(Operation op, UnboundTerm<Long> term, IndexRangeSet rangeSet) {
    super(op, term, Literal.of(0L));
    this.rangeSet = rangeSet;
  }

  public IndexRangeSet getRangeSet() {
    return rangeSet;
  }

  @Override
  public Expression bind(Types.StructType struct, boolean caseSensitive) {
    BoundTerm<Long> boundTerm = term().bind(struct, caseSensitive);
    return new BoundRangePredicate(op(), boundTerm, rangeSet);
  }
}
