/**
 * Copyright 2014 Mike Pigott
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mpigott.avro.xml;

import java.util.ArrayList;

import org.apache.avro.Schema;

/**
 * Represents information about an Avro type
 * representing a node in the document hierarchy.
 *
 * <p>
 * A future version will keep track of records in the
 * document that have IDREFs referencing this one.
 * </p>
 *
 * @author  Mike Pigott
 */
final class AvroRecordInfo {

  /**
   * Constructs a new <code>AvroRecordInfo</code> from its {@link Schema}.
   */
  public AvroRecordInfo(Schema avroSchema) {
    this.avroSchema = avroSchema;
    this.unionIndex = -1;
    this.numChildren = 0;
    this.mapSizes = null;
  }

  public AvroRecordInfo(Schema avroSchema, int unionIndex) {
    this.avroSchema = avroSchema;
    this.unionIndex = unionIndex;
    this.numChildren = 0;
    this.mapSizes = null;
  }

  Schema getAvroSchema() {
    return avroSchema;
  }

  int getUnionIndex() {
    return unionIndex;
  }

  int getNumChildren() {
    return numChildren;
  }

  void incrementChildCount() {
    ++numChildren;
  }

  void startNewMapInstance() {
    if (mapSizes == null) {
      mapSizes = new ArrayList<Integer>(4);
    }
    mapSizes.add(0);
  }

  void incrementMapCount() {
    if (mapSizes == null) {
      throw new IllegalStateException("Must call startNewMapInstance() first");
    }

    final int count = mapSizes.get(mapSizes.size() - 1);
    mapSizes.set(mapSizes.size() - 1, count + 1);
  }

  int getNumMapInstances() {
    return (mapSizes == null) ? 0 : mapSizes.size();
  }

  int getMapCountForInstance(int index) {
    if ((index < 0) || (mapSizes == null) || (mapSizes.size() <= index)) {
      throw new IllegalArgumentException("Requested map count for invalid index of " + index + "; map size is " + ((mapSizes == null) ? -1 : mapSizes.size()) + ".");
    }
    return mapSizes.get(index);
  }

  private final Schema avroSchema;
  private final int unionIndex;

  private int numChildren;

  private ArrayList<Integer> mapSizes;
}
