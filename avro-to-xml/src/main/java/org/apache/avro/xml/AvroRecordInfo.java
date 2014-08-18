/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.xml;

import org.apache.avro.Schema;

/**
 * Represents information about an Avro type
 * representing a node in the document hierarchy.
 */
final class AvroRecordInfo {

  private final Schema avroSchema;
  private final int unionIndex;
  private final int mapUnionIndex;

  private int numChildren;

  public AvroRecordInfo(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.unionIndex = -1;
    this.mapUnionIndex = -1;
    this.numChildren = 0;
  }

  public AvroRecordInfo(Schema avroSchema, int unionIndex, int mapUnionIndex) {
    this.avroSchema = avroSchema;
    this.unionIndex = unionIndex;
    this.mapUnionIndex = mapUnionIndex;
    this.numChildren = 0;
  }

  Schema getAvroSchema() {
    return avroSchema;
  }

  int getUnionIndex() {
    return unionIndex;
  }

  int getMapUnionIndex() {
    return mapUnionIndex;
  }

  int getNumChildren() {
    return numChildren;
  }

  void incrementChildCount() {
    ++numChildren;
  }
}
