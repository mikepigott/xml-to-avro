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
final class DocumentPathNode {

  DocumentPathNode(SchemaStateMachineNode node) {
    schemaNode = node;
    nextNodeStateIndex = -1;
    iterationNum = 0;
    prevNode = null;
    nextNode = null;
  }

  DocumentPathNode(DocumentPathNode previous, SchemaStateMachineNode node) {
    this(node);
    prevNode = previous;
  }

  DocumentPathNode(
      DocumentPathNode previous,
      DocumentPathNode copyOf) {

    schemaNode = copyOf.schemaNode;
    nextNodeStateIndex = -1;
    prevNode = previous;
    nextNode = null;
  }

  DocumentPathNode(
      DocumentPathNode previous,
      DocumentPathNode copyOf,
      int iteration) {

    this(previous, copyOf);
    iterationNum = iteration;
  }

  /**
   * Generates a hash code to represent this <code>DocumentPathNode</code>.
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + iterationNum;
    result = prime * result + ((nextNode == null) ? 0 : nextNode.hashCode());
    result = prime * result + nextNodeStateIndex;
    result = prime * result + ((prevNode == null) ? 0 : prevNode.hashCode());
    result = prime * result
        + ((schemaNode == null) ? 0 : schemaNode.hashCode());
    return result;
  }

  /**
   * Compares this to another <code>DocumentPathNode</code> for equality.
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
    if (!(obj instanceof DocumentPathNode)) {
      return false;
    }
    final DocumentPathNode other = (DocumentPathNode) obj;
    if (iterationNum != other.iterationNum) {
      return false;
    }
    if (nextNode == null) {
      if (other.nextNode != null) {
        return false;
      }
    } else if (!nextNode.equals(other.nextNode)) {
      return false;
    }
    if (nextNodeStateIndex != other.nextNodeStateIndex) {
      return false;
    }
    if (prevNode == null) {
      if (other.prevNode != null) {
        return false;
      }
    } else if (!prevNode.equals(other.prevNode)) {
      return false;
    }
    if (schemaNode == null) {
      if (other.schemaNode != null) {
        return false;
      }
    } else if (!schemaNode.equals(other.schemaNode)) {
      return false;
    }
    return true;
  }

  SchemaStateMachineNode getStateMachineNode() {
    return schemaNode;
  }

  int getIndexOfNextNodeState() {
    return nextNodeStateIndex;
  }

  int getIteration() {
    return iterationNum;
  }

  DocumentPathNode getPrevious() {
    return prevNode;
  }

  DocumentPathNode getNext() {
    return nextNode;
  }

  void setIteration(int newIteration) {
    iterationNum = newIteration;
  }

  DocumentPathNode setNextNode(int nextNodeIndex, DocumentPathNode newNext) {
    if ((nextNodeIndex < 0)
        || (nextNodeIndex >= schemaNode.getPossibleNextStates().size())) {
      throw new IllegalArgumentException("The node index (" + nextNodeIndex + ") is not within the range of " + schemaNode.getPossibleNextStates().size() + " possible next states.");

    } else if (newNext == null) {
      throw new IllegalArgumentException("The next node must be defined.");

    } else if ( !schemaNode
                   .getPossibleNextStates()
                   .get(nextNodeIndex)
                   .equals( newNext.getStateMachineNode() ) ) {

      throw new IllegalArgumentException("The next possible state at index " + nextNodeIndex + " does not match the state defined in the newNext.");
    }

    nextNodeStateIndex = nextNodeIndex;

    final DocumentPathNode oldNext = nextNode;
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
  DocumentPathNode setPreviousNode(DocumentPathNode newPrevious) {
    final DocumentPathNode oldPrevious = prevNode;
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
  DocumentPathNode update(
      DocumentPathNode newPrevious,
      SchemaStateMachineNode newNode) {

    schemaNode = newNode;
    nextNodeStateIndex = -1;
    iterationNum = 0;

    prevNode = newPrevious;

    final DocumentPathNode oldNext = nextNode;
    nextNode = null;

    return oldNext;
  }

  private SchemaStateMachineNode schemaNode;
  private int nextNodeStateIndex;
  private int iterationNum;

  private DocumentPathNode prevNode;
  private DocumentPathNode nextNode;
}
