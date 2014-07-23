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
    elemName = null;
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
    elemName = copyOf.elemName;
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

  SchemaStateMachineNode getStateMachineNode() {
    return schemaNode;
  }

  int getIndexOfNextNodeState() {
    return nextNodeStateIndex;
  }

  int getIteration() {
    return iterationNum;
  }

  QName getElementName() {
    return elemName;
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

    DocumentPathNode oldNext = nextNode;
    nextNode = newNext;

    return oldNext;
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
    elemName = null;
    iterationNum = 0;

    prevNode = newPrevious;

    DocumentPathNode oldNext = nextNode;
    nextNode = null;

    return oldNext;
  }

  /**
   * Use this method when changing the the {@link SchemaStateMachineNode}
   * and element this <code>DocumentPathNode</code> refers to.  The next
   * node in the path is returned, as it will be discarded internally.
   *
   * @param newPrevious    The new prior node this will be traversed from.
   * @param newNode        The new {@link SchemaStateMachineNode}.
   * @param newElementName The new element name.
   * @return The old next node in the chain, as it is discarded internally.
   */
  DocumentPathNode update(
      DocumentPathNode newPrevious,
      SchemaStateMachineNode newNode,
      QName newElementName) {

    DocumentPathNode oldPrev = update(newPrevious, newNode);
    elemName = newElementName;

    return oldPrev;
  }

  private SchemaStateMachineNode schemaNode;
  private int nextNodeStateIndex;
  private QName elemName;
  private int iterationNum;

  private DocumentPathNode prevNode;
  private DocumentPathNode nextNode;
}
