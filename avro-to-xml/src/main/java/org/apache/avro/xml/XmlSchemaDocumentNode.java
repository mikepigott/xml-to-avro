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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * The <code>XmlSchemaDocumentNode</code> represents a node in the
 * XML Schema as it is used by an XML document.  As {@link XmlSchemaPathFinder}
 * walks through an XML document, it builds {@link XmlSchemaPathNode}s
 * representing the path walked, and <code>XmlSchemaDocumentNode</code>s
 * representing where the XML document's elements fall in the XML Schema's
 * sequences, choices, and all groups.
 */
final class XmlSchemaDocumentNode<U> {

  private XmlSchemaStateMachineNode stateMachineNode;
  private XmlSchemaDocumentNode parent;
  private List<SortedMap<Integer, XmlSchemaDocumentNode<U>>> children;
  private List<XmlSchemaPathNode> visitors;
  private boolean receivedContent;
  private U userDefinedContent;

  XmlSchemaDocumentNode(
      XmlSchemaDocumentNode parent,
      XmlSchemaStateMachineNode stateMachineNode) {

    userDefinedContent = null;
    set(parent, stateMachineNode);
  }

  XmlSchemaStateMachineNode getStateMachineNode() {
    return stateMachineNode;
  }

  XmlSchemaDocumentNode getParent() {
    return parent;
  }

  SortedMap<Integer, XmlSchemaDocumentNode<U>> getChildren() {
    if (children == null) {
      return null;
    } else {
      return getChildren(children.size());
    }
  }

  SortedMap<Integer, XmlSchemaDocumentNode<U>> getChildren(int iteration) {
    if ((children == null)
          || (children.size() < iteration)
          || (iteration < 1)) {
      return null;
    } else {
      return children.get(iteration - 1);
    }
  }

  /**
   * Indicates whether an element has text in it.
   */
  boolean getReceivedContent() {
    return receivedContent;
  }

  void setReceivedContent(boolean receivedContent) {
    this.receivedContent = receivedContent;
  }

  /**
   * A visitor is a CHILD or SIBLING {@link XmlSchemaPathNode} entering
   * this <code>XmlSchemaDocumentNode</code>.  This is used to keep track
   * of how many occurrences are active via the current path winding
   * through the schema.
   */
  void addVisitor(XmlSchemaPathNode path) {
    if (path.getDocumentNode() != this) {
      throw new IllegalArgumentException(
          "Path node must have this XmlSchemaDocumentNode "
          + "as its document node.");
    }

    switch( path.getDirection() ) {
    case CHILD:
    case SIBLING:
      break;
    default:
      throw new IllegalArgumentException(
          "Only CHILD and SIBLING paths may be visitors of an "
          + "XmlSchemaDocumentNode, not a "
          + path.getDirection()
          + " path.");
    }

    if (visitors == null) {
      visitors = new ArrayList<XmlSchemaPathNode>(4);
    }

    if (children != null) { 
      if (children.size() == visitors.size()) {
        children.add( new TreeMap<Integer, XmlSchemaDocumentNode<U>>() );
      } else {
        throw new IllegalStateException("Attempted to add a new visitor when the number of occurrences (" + children.size() + ") did not match the number of existing visitors (" + visitors.size() + ").");
      }
    }

    visitors.add(path);
  }

  boolean removeVisitor(XmlSchemaPathNode path) {
    if ((visitors == null) || visitors.isEmpty()) {
      return false;
    }

    if ((children != null) && (visitors.size() != children.size())) {
      throw new IllegalStateException(
          "The number of visitors ("
          + visitors.size()
          + ") does not match the number of occurrences ("
          + children.size()
          + ").");
    }

    int visitorIndex = 0;
    for (; visitorIndex < visitors.size(); ++visitorIndex) {
      if (visitors.get(visitorIndex) == path) {
        break;
      }
    }

    if (visitors.size() == visitorIndex) {
      return false;
    }

    visitors.remove(visitorIndex);

    if (children != null) {
      children.remove(visitorIndex);
    }

    return true;
  }

  int getIteration() {
    if ((children != null) && (children.size() != visitors.size())) {
      throw new IllegalStateException(
          "The number of occurrences ("
          + children.size()
          + ") is not equal to the number of visitors ("
          + visitors.size()
          + ").");
    }
    return visitors.size();
  }

  long getMinOccurs() {
    return stateMachineNode.getMinOccurs();
  }

  long getMaxOccurs() {
    return stateMachineNode.getMaxOccurs();
  }

  int getSequencePosition() {
    if ((children == null)
        || (!stateMachineNode
              .getNodeType()
              .equals(XmlSchemaStateMachineNode.Type.SEQUENCE))) {
      return -1;
    } else if (children.isEmpty()) {
      return 0;
    } else if (children.get(children.size() - 1).isEmpty()) {
      return 0;
    } else {
      return children.get(children.size() - 1).lastKey();
    }
  }

  final void set(
      XmlSchemaDocumentNode parent,
      XmlSchemaStateMachineNode stateMachineNode) {

    this.parent = parent;
    this.stateMachineNode = stateMachineNode;
    this.receivedContent = false;
    this.visitors = null;

    if ((this.stateMachineNode.getPossibleNextStates() == null)
        || this.stateMachineNode.getPossibleNextStates().isEmpty()) {
      this.children = null;

    } else {
      this.children =
          new ArrayList<SortedMap<Integer, XmlSchemaDocumentNode<U>>>(1);
    }
  }

  U getUserDefinedContent() {
    return userDefinedContent;
  }

  void setUserDefinedContent(U userDefinedContent) {
    this.userDefinedContent = userDefinedContent;
  }
}