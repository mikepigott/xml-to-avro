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

  /**
   * 
   */
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

  SchemaStateMachineNode getStateMachineNode() {
    return schemaNode;
  }

  int getIndexOfNextNodeState() {
    return nextNodeStateIndex;
  }

  QName getElementName() {
    return elemName;
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
  DocumentPathNode update(DocumentPathNode newPrevious, SchemaStateMachineNode newNode) {
    schemaNode = newNode;
    nextNodeStateIndex = -1;
    elemName = null;
    iterationNum = 0;

    prevNode = newPrevious;

    DocumentPathNode oldNext = nextNode;
    nextNode = null;

    return oldNext;
  }

  private SchemaStateMachineNode schemaNode;
  private int nextNodeStateIndex;
  private QName elemName;
  private int iterationNum;

  private DocumentPathNode prevNode;
  private DocumentPathNode nextNode;
}
