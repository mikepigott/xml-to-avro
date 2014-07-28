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

/**
 * Factory for creating {@link XmlSchemaPathNode}s.  This allows
 * for recyling and abstracts away the complexity of walking through an
 * XML Schema.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaPathManager {

  /**
   * Constructs the document path node factory.
   */
  public XmlSchemaPathManager() {
    unusedPathNodes = new ArrayList<XmlSchemaPathNode>();
  }

  XmlSchemaPathNode createStartPathNode(
      XmlSchemaPathNode.Direction direction,
      XmlSchemaStateMachineNode state) {

    return createPathNode(direction, null, state);
  }

  XmlSchemaPathNode createStartPathNode(
      XmlSchemaPathNode.Direction direction,
      XmlSchemaDocumentNode documentNode) {

    XmlSchemaPathNode node =
        createStartPathNode(direction, documentNode.getStateMachineNode());
    node.setDocumentNode(documentNode);

    return node;
  }

  XmlSchemaPathNode addParentSiblingOrContentNodeToPath(
      XmlSchemaPathNode startNode,
      XmlSchemaPathNode.Direction direction) {

    XmlSchemaDocumentNode position = startNode.getDocumentNode();

    switch (direction) {
    case PARENT:
      if (position != null) {
        position = position.getParent();
      }
    case SIBLING:
    case CONTENT:
      if (position == null) {
        throw new IllegalStateException("When calling addParentSiblingOrContentNodeToPath(), the startNode's document node (and its parent) cannot be null.");
      }
      break;
    default:
      throw new IllegalStateException("This method cannot be called if following a child.  Use addChildNodeToPath(startNode, direction, stateIndex).");
    }

    XmlSchemaPathNode node = null;
    if ( !unusedPathNodes.isEmpty() ) {
      node =
          unusedPathNodes.remove(unusedPathNodes.size() - 1);
      node.update(direction, startNode, position);
    } else {
      node = new XmlSchemaPathNode(direction, startNode, position);
    }

    return node;
  }

  XmlSchemaPathNode addChildNodeToPath(
      XmlSchemaPathNode startNode,
      XmlSchemaPathNode.Direction direction,
      int branchIndex) {

    if (!direction.equals(XmlSchemaPathNode.Direction.CHILD)) {
      throw new IllegalStateException("This method can only be called if following a child.  Use addParentSiblingOrContentNodeToPath(startNode, direction, position) instead.");
    }

    final XmlSchemaStateMachineNode stateMachine =
        startNode.getStateMachineNode();

    if (stateMachine.getPossibleNextStates() == null) {
      throw new IllegalStateException("Cannot follow the branch index");
    } else if (stateMachine.getPossibleNextStates().size() <= branchIndex) {
      throw new IllegalArgumentException("Cannot follow the branch index; ");
    }

    final XmlSchemaPathNode next =
        createPathNode(
            direction,
            startNode,
            stateMachine.getPossibleNextStates().get(branchIndex));

    final XmlSchemaDocumentNode docNode = startNode.getDocumentNode();
    if ((startNode.getDocumentNode() != null)
        && (docNode.getChildren() != null)
        && (docNode.getChildren().size() > branchIndex)) {

      next.setDocumentNode( docNode.getChildren().get(branchIndex) );
    }

    return next;
  }

  XmlSchemaPathNode clone(XmlSchemaPathNode original) {
    XmlSchemaPathNode clone =
        createPathNode(
            original.getDirection(),
            original.getPrevious(),
            original.getStateMachineNode());

    if (original.getDocumentNode() != null) {
      clone.setDocumentNode( original.getDocumentNode() );
    }

    clone.setIteration(original.getIteration());

    return original;
  }

  /**
   * Recyles the provided {@link XmlSchemaPathNode} and all of
   * the nodes that follow it.  Unlinks from its previous node.
   */
  void recyclePathNode(XmlSchemaPathNode toRecycle) {
    toRecycle.getPrevious().setNextNode(-1, null);
    toRecycle.setPreviousNode(null);

    if (toRecycle.getNext() != null) {
      recyclePathNode( toRecycle.getNext() );
    }

    unusedPathNodes.add(toRecycle);
  }

  private XmlSchemaPathNode createPathNode(
      XmlSchemaPathNode.Direction direction,
      XmlSchemaPathNode previous,
      XmlSchemaStateMachineNode state) {
    
    if ( !unusedPathNodes.isEmpty() ) {
      XmlSchemaPathNode node =
          unusedPathNodes.remove(unusedPathNodes.size() - 1);
      node.update(direction, previous, state);
      return node;
    } else {
      return new XmlSchemaPathNode(direction, previous, state);
    }
  }

  private ArrayList<XmlSchemaPathNode> unusedPathNodes;
}
