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
import java.util.Map;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.parquet.VectorizedReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/** A base BatchReader class that contains common functionality */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class BaseBatchReader<T> implements VectorizedReader<T> {
  protected final VectorizedReader<VectorHolder>[] readers;
  protected final VectorHolder[] vectorHolders;

  @SuppressWarnings("unchecked")
  protected BaseBatchReader(List<VectorizedReader<?>> readers) {
    this.readers = new VectorizedReader[readers.size()];
    for (int k = 0; k < this.readers.length; k++) {
      this.readers[k] = (VectorizedReader<VectorHolder>) readers.get(k);
    }
    this.vectorHolders = new VectorHolder[readers.size()];
  }

  @Override
  public void setRowGroupInfo(
      PageReadStore pageStore, Map<ColumnPath, ColumnChunkMetaData> metaData, long rowPosition) {
    for (VectorizedReader<VectorHolder> reader : readers) {
      if (reader != null) {
        reader.setRowGroupInfo(pageStore, metaData, rowPosition);
      }
    }
  }

  protected void closeVectors() {
    for (int i = 0; i < vectorHolders.length; i++) {
      if (vectorHolders[i] != null) {
        // Release any resources used by the vector
        FieldVector vec = vectorHolders[i].vector();
        if (vec != null) {
          vec.close();
        }
        vectorHolders[i] = null;
      }
    }
  }

  @Override
  public void close() {
    for (VectorizedReader<?> reader : readers) {
      if (reader != null) {
        reader.close();
      }
    }
    closeVectors();
  }

  @Override
  public void setBatchSize(int batchSize) {
    for (VectorizedReader<VectorHolder> reader : readers) {
      if (reader != null) {
        reader.setBatchSize(batchSize);
      }
    }
  }
}
