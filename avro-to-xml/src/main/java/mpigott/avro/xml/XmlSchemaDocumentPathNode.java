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
 * This represents a node in the path when walking an XML or Avro document.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaDocumentPathNode {

  enum Direction {
    PARENT,
    CHILD,
    CONTENT
  }

  XmlSchemaDocumentPathNode(Direction dir, XmlSchemaDocumentNode node) {
    direction = dir;
    schemaNode = node;
    nextNodeStateIndex = -1;
    priorSequencePosition = node.getCurrPositionInSequence();
    iterationNum = 0;
    prevNode = null;
    nextNode = null;
  }

  XmlSchemaDocumentPathNode(
      Direction dir,
      XmlSchemaDocumentPathNode previous,
      XmlSchemaDocumentNode node) {

    this(dir, node);
    prevNode = previous;
  }

  /**
   * Generates a hash code to represent this <code>DocumentPathNode</code>.
   * This does not perform a deep search, as that would be expensive for long
   * paths.  This only checks the hash codes of the local data members of its
   * neighbors.
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = localHashCode(prime);
    result =
        prime * result
        + ((nextNode == null) ? 0 : nextNode.localHashCode(prime));

    result =
        prime * result
        + ((prevNode == null) ? 0 : prevNode.localHashCode(prime));

    return result;
  }

  /**
   * Compares this to another <code>DocumentPathNode</code> for equality.
   * This does not perform a deep equality check, as that would be expensive
   * for long paths.  This only checks the equality of local data members of
   * its neighbors.
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof XmlSchemaDocumentPathNode)) {
      return false;
    }
    final XmlSchemaDocumentPathNode other = (XmlSchemaDocumentPathNode) obj;
    if (!localEquals(other)) {
      return false;
    }
    if (nextNode == null) {
      if (other.nextNode != null) {
        return false;
      }
    } else if (!nextNode.localEquals(other.nextNode)) {
      return false;
    }
    if (prevNode == null) {
      if (other.prevNode != null) {
        return false;
      }
    } else if (!prevNode.localEquals(other.prevNode)) {
      return false;
    }
    return true;
  }

  XmlSchemaDocumentNode getDocumentNode() {
    return schemaNode;
  }

  SchemaStateMachineNode getStateMachineNode() {
    return schemaNode.getStateMachineNode();
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

  /**
   * When unfollowing a <code>XmlSchemaDocumentPathNode</code>, we need to
   * reset the child position of a {@link XmlSchemaDocumentNode} representing
   * an XML Schema Sequence group.
   */
  int getPriorSequencePosition() {
    return priorSequencePosition;
  }

  XmlSchemaDocumentPathNode getPrevious() {
    return prevNode;
  }

  XmlSchemaDocumentPathNode getNext() {
    return nextNode;
  }

  void setIteration(int newIteration) {
    iterationNum = newIteration;
  }

  XmlSchemaDocumentPathNode setNextNode(
      int nextNodeIndex,
      XmlSchemaDocumentPathNode newNext) {

    if ((nextNodeIndex == -1)
        && (newNext.getDirection().equals(Direction.CONTENT)
            || newNext.getDirection().equals(Direction.PARENT))) {

      /* If this is a content node; no validation is needed because
       * we didn't change our position in the document tree.
       *
       * If this is a parent node, no validation is possible because
       * we do not track the prior state in the state machine.
       */

    } else if ((nextNodeIndex < 0)
                || (nextNodeIndex
                    >= schemaNode
                         .getStateMachineNode()
                         .getPossibleNextStates().size())) {
      throw new IllegalArgumentException("The node index (" + nextNodeIndex + ") is not within the range of " + schemaNode.getStateMachineNode().getPossibleNextStates().size() + " possible next states.");

    } else if (newNext == null) {
      throw new IllegalArgumentException("The next node must be defined.");

    } else if ( !schemaNode
                   .getStateMachineNode()
                   .getPossibleNextStates()
                   .get(nextNodeIndex)
                   .equals( newNext.getStateMachineNode() ) ) {

      throw new IllegalArgumentException("The next possible state at index " + nextNodeIndex + " does not match the state defined in the newNext.");
    }

    nextNodeStateIndex = nextNodeIndex;

    final XmlSchemaDocumentPathNode oldNext = nextNode;
    nextNode = newNext;

    return oldNext;
  }

  /**
   * Changes the previous node this one was pointing to.
   * This is useful when cloning prior nodes in the chain.
   *
   * @param newPrevious The new previous node.
   * @return The old previous node.
   */
  XmlSchemaDocumentPathNode setPreviousNode(XmlSchemaDocumentPathNode newPrevious) {
    final XmlSchemaDocumentPathNode oldPrevious = prevNode;
    prevNode = newPrevious;
    return oldPrevious;
  }

  /**
   * Use this method when changing the the {@link SchemaStateMachineNode}
   * this <code>DocumentPathNode</code> refers to.  The next node in the
   * path is returned, as it will be discarded internally.
   *
   * @param newPrevious The new previous <code>DocumentPathNode</code> this
   *                    node is traversed from.
   *
   * @param newNode The new {@link SchemaStateMachineNode} this node refers to.
   *
   * @return The next node in the path that this node referred to, as it will
   *         be discarded internally. 
   */
  XmlSchemaDocumentPathNode update(
      XmlSchemaDocumentPathNode.Direction newDirection,
      XmlSchemaDocumentPathNode newPrevious,
      XmlSchemaDocumentNode newNode) {

    direction = newDirection;
    schemaNode = newNode;
    nextNodeStateIndex = -1;
    priorSequencePosition = newNode.getCurrPositionInSequence();
    iterationNum = 0;

    prevNode = newPrevious;

    final XmlSchemaDocumentPathNode oldNext = nextNode;
    nextNode = null;

    return oldNext;
  }

  private int localHashCode(int prime) {
    int result = 1;
    result = prime * result + iterationNum;
    result = prime * result + nextNodeStateIndex;
    result = prime * result + priorSequencePosition;
    result = prime * result + ((direction == null) ? 0 : direction.hashCode());
    result = prime * result
        + ((schemaNode == null) ? 0 : schemaNode.hashCode());
    return result;
  }

  private boolean localEquals(XmlSchemaDocumentPathNode other) {
    if (direction != other.direction) {
      return false;
    }
    if (iterationNum != other.iterationNum) {
      return false;
    }
    if (nextNodeStateIndex != other.nextNodeStateIndex) {
      return false;
    }
    if (priorSequencePosition != other.priorSequencePosition) {
      return false;
    }
    if (schemaNode == null) {
      if (other.schemaNode != null) {
        return false;
      }
    } else if (!schemaNode.equals(other.schemaNode)) {
      return false;
    }
    return false;
  }

  private Direction direction;
  private XmlSchemaDocumentNode schemaNode;
  private int nextNodeStateIndex;
  private int priorSequencePosition;
  private int iterationNum;

  private XmlSchemaDocumentPathNode prevNode;
  private XmlSchemaDocumentPathNode nextNode;
}
