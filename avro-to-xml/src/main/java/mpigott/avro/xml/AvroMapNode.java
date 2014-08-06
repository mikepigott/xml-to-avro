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

import javax.xml.namespace.QName;

/**
 * Information about a {@link XmlSchemaPathNode} representing an Avro MAP.
 *
 * @author  Mike Pigott
 */
final class AvroMapNode {

  enum Type {
    MAP_START,
    ITEM_START,
    MAP_END
  }

  AvroMapNode(
      XmlSchemaPathNode<AvroRecordInfo, AvroMapNode> pathNode,
      int pathIndex,
      Type type,
      QName qName,
      int occurrence) {

    this.pathNode = pathNode;
    this.pathIndex = pathIndex;
    this.qName = qName;
    this.type = type;
    this.occurrence = occurrence;
    this.mapSize = -1;
  }

  AvroMapNode(
      XmlSchemaPathNode<AvroRecordInfo, AvroMapNode> pathNode,
      int pathIndex,
      Type type) {

    this(pathNode, pathIndex, type, null, 0);
  }

  QName getQName() {
    if (qName != null) {
      return qName;
    } else if (pathNode
                 .getStateMachineNode()
                 .getNodeType()
                 .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      return pathNode.getStateMachineNode().getElement().getQName();
    } else {
      return null;
    }
  }

  XmlSchemaPathNode<AvroRecordInfo, AvroMapNode> getPathNode() {
    return pathNode;
  }

  int getPathIndex() {
    return pathIndex;
  }

  int getOccurrence() {
    return occurrence;
  }

  Type getType() {
    return type;
  }

  int getMapSize() {
    return mapSize;
  }

  void setMapSize(int mapSize) {
    this.mapSize = mapSize;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("[");
    str.append(type).append(": ").append(pathIndex);
    str.append(" ").append( pathNode.getDirection() );
    str.append(" ").append( pathNode.getStateMachineNode() );
    str.append("]");
    return str.toString();
  }

  private final XmlSchemaPathNode<AvroRecordInfo, AvroMapNode> pathNode;
  private final int pathIndex;
  private final QName qName;
  private final int occurrence;
  private final Type type;

  private int mapSize;
}
