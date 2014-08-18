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

import javax.xml.namespace.QName;

/**
 * Information about a {@link XmlSchemaPathNode} representing
 * an Avro MAP or a CONTENT node in a mixed-content element.
 *
 * <p>
 * When XML elements are best represented by a map, sibling elements are
 * grouped together under one common MAP instance.  This keeps track of
 * whether a new element represents the start of a new MAP or the start
 * of an item in an existing MAP.  A later path node will signify when
 * the map ends.
 * </p>
 *
 * <p>
 * We also need to keep track of the content nodes that are part of mixed
 * elements, as they will be added to the corresponding RECORD's ARRAY of
 * children.
 * </p>
 */
final class AvroPathNode {

  private final XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> pathNode;
  private final QName qName;
  private final int occurrence;
  private final Type type;
  private final int contentUnionIndex;

  private int mapSize;

  enum Type {
    MAP_START,
    ITEM_START,
    MAP_END,
    CONTENT
  }

  /**
   * Constructor for creating a new {@link AvroPathNode} to represent a MAP.
   *
   * @param pathNode  The path node representing the start or end of the map
   *                  or one if its items.
   *
   * @param pathIndex The index of the path itself.
   * @param type
   * @param qName
   * @param occurrence
   */
  AvroPathNode(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> pathNode,
      Type type,
      QName qName,
      int occurrence) {

    if (type.equals(Type.CONTENT)) {
      throw new IllegalArgumentException(
          "Use the AvroPathNode(unionIndex) constructor "
          + "for adding CONTENT nodes.");
    }

    this.pathNode = pathNode;
    this.qName = qName;
    this.type = type;
    this.occurrence = occurrence;
    this.mapSize = -1;
    this.contentUnionIndex = -1;
  }

  AvroPathNode(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> pathNode,
      Type type) {

    this(pathNode, type, null, 0);
  }

  AvroPathNode(int unionIndex) {
    this.type = Type.CONTENT;
    this.contentUnionIndex = unionIndex;

    this.pathNode = null;
    this.qName = null;
    this.occurrence = -1;
    this.mapSize = -1;
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

  XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> getPathNode() {
    return pathNode;
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

  int getContentUnionIndex() {
    return contentUnionIndex;
  }

  @Override
  public String toString() {
    StringBuilder str = new StringBuilder("[");
    str.append(type).append(": ").append( pathNode.getDirection() );
    str.append(" ").append( pathNode.getStateMachineNode() );
    str.append("]");
    return str.toString();
  }
}
