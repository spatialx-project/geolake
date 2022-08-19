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
package org.apache.iceberg.data;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

class TableScanIterable extends CloseableGroup implements CloseableIterable<Record> {
  private final GenericReader reader;
  private final CloseableIterable<CombinedScanTask> tasks;
  private final Map<String, String> properties;

  TableScanIterable(TableScan scan, boolean reuseContainers, Map<String, String> properties) {
    this.reader = new GenericReader(scan, reuseContainers);
    // start planning tasks in the background
    this.tasks = scan.planTasks();
    this.properties = properties;
  }

  @Override
  public CloseableIterator<Record> iterator() {
    CloseableIterator<Record> iter = reader.open(tasks, properties);
    addCloseable(iter);
    return iter;
  }

  @Override
  public void close() throws IOException {
    tasks.close(); // close manifests from scan planning
    super.close(); // close data files
  }
}
