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

/**
 * Information about a {@link XmlSchemaPathNode} representing an Avro MAP.
 *
 * @author  Mike Pigott
 */
final class AvroMapNode {

  enum Type {
    START,
    MIDDLE,
    END
  }

  /**
   * Creates a new <code>AvroMapNode</code>.
   */
  AvroMapNode(
      XmlSchemaPathNode pathNode,
      int pathIndex,
      Type type) {

    this.pathNode = pathNode;
    this.pathIndex = pathIndex;
    this.type = type;
  }

  XmlSchemaPathNode getPathNode() {
    return pathNode;
  }

  int getPathIndex() {
    return pathIndex;
  }

  Type getType() {
    return type;
  }

  private final XmlSchemaPathNode pathNode;
  private final int pathIndex;
  private final Type type;
}
