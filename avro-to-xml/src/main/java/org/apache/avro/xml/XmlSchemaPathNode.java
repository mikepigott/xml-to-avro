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

/**
 * This represents a node in the path when walking an XML or Avro document.
 * As {@link XmlSchemaPathFinder} walks through an XML document, it builds
 * <code>XmlSchemaPathNode</code>s  representing the path walked, and
 * {@link XmlSchemaDocumentNode}s representing where the XML document's
 * elements fall in the XML Schema's sequences, choices, and all groups.
 */
final class XmlSchemaPathNode<U, V> {

  private Direction direction;
  private XmlSchemaDocumentNode<U> documentNode;
  private XmlSchemaStateMachineNode stateMachineNode;
  private int nextNodeStateIndex;
  private int iterationNum;
  private V userDefinedContent;

  private XmlSchemaPathNode<U, V> prevNode;
  private XmlSchemaPathNode<U, V> nextNode;

  enum Direction {
    PARENT(2),
    CHILD(0),
    CONTENT(3),
    SIBLING(1);

    // CHILD < SIBLING < PARENT < CONTENT when comparing possible paths.
    Direction (int rank) {
      this.rank = rank;
    }

    int getRank() {
      return rank;
    }

    final int rank;
  }

  XmlSchemaPathNode(
      Direction dir,
      XmlSchemaPathNode previous,
      XmlSchemaDocumentNode<U> node) {

    update(dir, previous, node);
  }

  XmlSchemaPathNode(
      Direction dir,
      XmlSchemaPathNode previous,
      XmlSchemaStateMachineNode stateMachine) {

    update(dir, previous, stateMachine);
  }

  XmlSchemaDocumentNode<U> getDocumentNode() {
    return documentNode;
  }

  XmlSchemaStateMachineNode getStateMachineNode() {
    return stateMachineNode;
  }

  Direction getDirection() {
    return direction;
  }

  int getIndexOfNextNodeState() {
    return nextNodeStateIndex;
  }

  int getIteration() {
    return iterationNum;
  }

  long getMinOccurs() {
    return stateMachineNode.getMinOccurs();
  }

  long getMaxOccurs() {
    return stateMachineNode.getMaxOccurs();
  }

  int getDocIteration() {
    if (documentNode == null) {
      return 0;
    } else {
      return documentNode.getIteration();
    }
  }

  int getDocSequencePosition() {
    if (documentNode == null) {
      return 0;
    } else {
      return documentNode.getSequencePosition();
    }
  }

  XmlSchemaPathNode getPrevious() {
    return prevNode;
  }

  XmlSchemaPathNode getNext() {
    return nextNode;
  }

  void setIteration(int newIteration) {
    if (newIteration < 1) {
      throw new IllegalArgumentException(
          "The new iteration must be at least one, not "
          + newIteration
          + '.');

    } else if (stateMachineNode.getMaxOccurs() < newIteration) {
      throw new IllegalStateException(
          "The new iteration for "
          + stateMachineNode
          + " of "
          + newIteration
          + " is greater than the maximum of "
          + stateMachineNode.getMaxOccurs()
          + '.');
    }
    iterationNum = newIteration;
  }

  void setDocumentNode(XmlSchemaDocumentNode<U> docNode) {
    if (docNode.getStateMachineNode() != stateMachineNode) {
      throw new IllegalArgumentException(
          "The document node's state machine ("
          + docNode.getStateMachineNode()
          + ") must use the same state machine node as this path node ("
          + stateMachineNode
          + ").");
    }
    documentNode = docNode;
  }

  void setNextNode(int nextNodeIndex, XmlSchemaPathNode newNext) {

    if ((nextNodeIndex == -1)
        && ((newNext == null)
            || newNext.getDirection().equals(Direction.CONTENT)
            || newNext.getDirection().equals(Direction.PARENT)
            || newNext.getDirection().equals(Direction.SIBLING))) {

      /* If this is either a content node or a sibling node, no validation is
       * needed because we didn't change our position in the document tree.
       *
       * If this is a parent node, no validation is possible because
       * we do not track the prior state in the state machine.
       *
       * We can also reset lower nodes in the path as necessary.
       */

    } else if ((nextNodeIndex < 0)
                || (nextNodeIndex
                    >= stateMachineNode
                         .getPossibleNextStates().size())) {
      throw new IllegalArgumentException(
          "The node index ("
          + nextNodeIndex
          + ") is not within the range of "
          + documentNode.getStateMachineNode().getPossibleNextStates().size()
          + " possible next states.");

    } else if (newNext == null) {
      throw new IllegalArgumentException("The next node must be defined.");

    } else if ( !stateMachineNode
                   .getPossibleNextStates()
                   .get(nextNodeIndex)
                   .equals( newNext.getStateMachineNode() ) ) {

      throw new IllegalArgumentException(
          "The next possible state at index "
          + nextNodeIndex
          + " does not match the state defined in the newNext.");
    }

    nextNodeStateIndex = nextNodeIndex;
    nextNode = newNext;
  }

  /**
   * Changes the previous node this one was pointing to.
   * This is useful when cloning prior nodes in the chain.
   *
   * @param newPrevious The new previous node.
   * @return The old previous node.
   */
  XmlSchemaPathNode setPreviousNode(XmlSchemaPathNode newPrevious) {
    final XmlSchemaPathNode<U, V> oldPrevious = prevNode;
    prevNode = newPrevious;
    return oldPrevious;
  }

  /**
   * Use this method when changing the the {@link XmlSchemaStateMachineNode}
   * this <code>DocumentPathNode</code> refers to.  The next node in the
   * path is returned, as it will be discarded internally.
   *
   * @param newPrevious The new previous <code>DocumentPathNode</code> this
   *                    node is traversed from.
   *
   * @param newNode The new {@link XmlSchemaStateMachineNode}
   *                this node refers to.
   *
   * @return The next node in the path that this node referred to, as it will
   *         be discarded internally. 
   */
  final void update(
      Direction newDirection,
      XmlSchemaPathNode newPrevious,
      XmlSchemaDocumentNode<U> newNode) {

    update(newDirection, newPrevious, newNode.getStateMachineNode());
    documentNode = newNode;
  }

  final void update(
      Direction newDirection,
      XmlSchemaPathNode newPrevious,
      XmlSchemaStateMachineNode newStateMachineNode) {

    direction = newDirection;
    documentNode = null;
    stateMachineNode = newStateMachineNode;
    nextNodeStateIndex = -1;
    iterationNum = 0;
    prevNode = newPrevious;
    nextNode = null;
    userDefinedContent = null;
  }

  V getUserDefinedContent() {
    return userDefinedContent;
  }

  void setUserDefinedContent(V content) {
    userDefinedContent = content;
  }
}
