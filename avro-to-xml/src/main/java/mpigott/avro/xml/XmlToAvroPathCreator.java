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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaContentProcessing;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Performs a SAX-based walk through the XML document, determining the
 * interpretation ("path") that best matches both the XML Schema and the
 * Avro Schema.
 *
 * @author  Mike Pigott
 */
final class XmlToAvroPathCreator extends DefaultHandler {

  /* We want to keep track of all of the valid path segments to a particular
   * element, but we do not want to stomp on the very first node until we
   * know which path we want to follow.  Likewise, we want to keep the
   * first node in the segment without a "next" node, but every node after
   * that we wish to chain together.
   *
   * To accomplish this, we start with a base node at the end and "prepend"
   * previous nodes until we work our way back to the beginning.  When we
   * prepend a node, we link the previous start node to the node directly
   * after it, while leaving the new start node unlinked.
   *
   * Path segments may also be recycled when a decision point is refuted.
   */
  private final class PathSegment {
    PathSegment() {
      start = null;
      end = null;
      length = 0;
    }

    PathSegment(DocumentPathNode node) {
      set(node);
    }

    int getLength() {
      if ((length == 0) && (start != end)) {
        for (DocumentPathNode iter = afterStart;
            iter != end;
            iter = iter.getNext()) {
          ++length;
        }
        ++length; // (afterStart -> end) + start
      }
      return length;
    }

    /* Prepends a new start node to this segment.  We want to clone
     * the previous start node as sibling paths may be sharing it.
     * We also need to know the newStart's path index to reach the
     * clonedStartNode, so we know how to properly link them later.
     */
    void prepend(DocumentPathNode newStart, int pathIndexToNextNode) {
      // We need to clone start and make it the afterStart.
      final DocumentPathNode clonedStartNode =
          createDocumentPathNode(
              start.getPrevious(),
              start.getStateMachineNode());

      if (afterStart != null) {
        afterStart.setPreviousNode(clonedStartNode);
        clonedStartNode.setNextNode(afterStartPathIndex, afterStart);
        afterStart = clonedStartNode;

      } else {
        // This path segment only has one node in it; now it has two.
        end = clonedStartNode;
        afterStart = clonedStartNode;
      }

      start = newStart;
      afterStartPathIndex = pathIndexToNextNode;
    }

    DocumentPathNode getStart() {
      return start;
    }

    DocumentPathNode getEnd() {
      return end;
    }

    DocumentPathNode getAfterStart() {
      return afterStart;
    }

    final void set(DocumentPathNode node) {
      this.start = node;
      this.end = node;
      this.afterStart = null;
      this.afterStartPathIndex = -1;
      this.length = 0;
    }

    private DocumentPathNode start;
    private DocumentPathNode end;
    private DocumentPathNode afterStart;
    private int length;
    private int afterStartPathIndex;
  }

  /**
   * A <code>DescisionPoint</code> is a location in a document path where
   * an element in the document can be reached by following two or more
   * different traversals through the XML Schema.
   *
   * When we reach such a decision point, we will keep track of the different
   * paths through the XML Schema that reach the element.  We will then follow
   * each path, one-by-one from the shortest through the longest, until we find
   * a path that successfully navigates both the document and the schema.
   */
  private static class DecisionPoint {
    DocumentPathNode decisionPoint;
    List<PathSegment> choices;
    int traversedElementIndex;
  }

  /**
   * Represents the state machine as a tree with the current iteration of each
   * node, and additional state information for All and Sequence groups.
   *
   * If the node represents a sequence group, we need to know which child we
   * visit next.  Once we visit a node the maximum number of occurrences (or
   * we visit the minimum number of occurrences and the element name does not
   * match), this index will be incremented to the next child.
   *
   * This class is package-protected, and not private, to allow an external
   * graph generator to build a visualization of the tree.
   */
  static class StateMachineTreeWithState {
    StateMachineTreeWithState(SchemaStateMachineNode stateMachineNode) {
      this.stateMachineNode = stateMachineNode;

      if ((this.stateMachineNode.getPossibleNextStates() == null)
          || this.stateMachineNode.getPossibleNextStates().isEmpty()) {
        this.children = null;

      } else {
        this.children =
            new ArrayList<StateMachineTreeWithState>(
                this.stateMachineNode.getPossibleNextStates().size() );
      }

      this.parent = null;
      this.currIteration = 0;
      this.currPositionInSeqGroup = -1;
    }

    StateMachineTreeWithState(
        StateMachineTreeWithState parent,
        SchemaStateMachineNode stateMachineNode) {

      this(stateMachineNode);
      this.parent = parent;
    }

    SchemaStateMachineNode stateMachineNode;
    StateMachineTreeWithState parent;
    List<StateMachineTreeWithState> children;

    int currIteration;
    int currPositionInSeqGroup;
  }

  /**
   * Creates a new <code>XmlToAvroPathCreator</code> with the root
   * {@link SchemaStateMachineNode} to start from when evaluating documents.
   */
  XmlToAvroPathCreator(SchemaStateMachineNode root) {
    rootNode = root;
    rootStateNode = new StateMachineTreeWithState(rootNode);
    rootPathNode = new DocumentPathNode(root);

    traversedElements = new ArrayList<QName>();
    currentPosition = null;
    currentPath = null;
    decisionPoints = null; // Hopefully there won't be any!

    unusedNodePool = null;
    unusedTreePool = null;
    unusedPathSegmentPool = null;
  }

  @Override
  public void startDocument() throws SAXException {
    currentPosition = rootStateNode;
    currentPath = rootPathNode;

    traversedElements.clear();

    if (decisionPoints != null) {
      decisionPoints.clear();
    }
  }

  /**
   * Find the path through the XML Schema that best matches this element.
   *
   * @see org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
   */
  @Override
  public void startElement(
      String uri,
      String localName,
      String qName,
      Attributes atts) throws SAXException {

    final QName elemQName = new QName(uri, localName);

    try {
      traversedElements.add(elemQName);
  
      final SchemaStateMachineNode state = currentPosition.stateMachineNode;
  
      // 1. Find possible paths.
      List<PathSegment> possiblePaths =
          find(currentPath, currentPosition, elemQName);
  
      if (possiblePaths != null) {
        // 2. Build path segments from those paths.
  
        /* 3. If multiple paths were returned, add a DecisionPoint.
         *    Sort the paths where paths ending in elements are favored over
         *    element wild cards, and shorter paths are favored over longer
         *    paths.
         */
  
        /* 4. Choose the highest-rank path and build the
         *    StateMachineTreeWithState accordingly.
         */
  
        /* 5. Confirm the attributes of the element
         *    match what is expected from the schema.
         */
  
      } else {
        // OR: If no paths are returned:
  
        /* 2a. Backtrack to the most recent decision point.
         *     Remove the top path (the one we just tried),
         *     and select the next one.
         */
  
        /* 3a. Walk through the traversedElements list again from that
         *     index and see if we traverse through all of the elements
         *     in the list, including this one.  If not, repeat step 2a,
         *     removing decision points from the stack as we refute them.
         */
  
        // 4a. If we find (a) path(s) that match(es), success!  Return to step 2.
  
        /* OR: If we go through all prior decision points and are unable to find
         *     one or more paths through the XML Schema that match the document,
         *     throw an error.  There is nothing more we can do here.
         *
         * TODO: When discarding an existing path segment, remember
         *       to walk through the tree and undo what the path did.
         */
      }
    } catch (Exception e) {
      throw new SAXException("Error occurred while starting element " + elemQName + "; traversed path is " + getElementsTraversedAsString(), e);
    }
  }

  /**
   * 
   * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
   */
  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {

    /* If the most recent path node is an element with simple content,
     * confirm these characters match the data type expected.
     *
     * If we are not expecting an element with simple content,
     * and the characters don't represent all whitespace, throw
     * an exception.
     */

    try {
      // Do stuff.
    } catch (Exception e) {
      throw new SAXException("Error occurred while processing characters; traversed path was " + getElementsTraversedAsString(), e);
    }
  }

  /**
   * Confirm the current position matches the element we are ending.
   * If not, throw an exception.
   *
   * If the number of occurrences is less than the minimum number of
   * occurrences, do not move.  The next element must be an instance
   * of this one.
   *
   * Otherwise, walk back up the tree to the next position.
   *
   * If the parent is a group of any kind, and its minimum number of
   * occurrences is not fulfilled, stop there.
   *
   * Otherwise, if the parent is a choice group or substitution group,
   * walk two levels up to the grandparent.  If the number of occurrences
   * of this element, the choice group, or the substitution group are
   * maxed out, and the grandparent is a sequence group or all group,
   * update the information accordingly.
   *
   * If the parent is a sequence group or an all group, update it
   * accordingly.  Again, if the number of occurrences is equal to
   * the maximum number, advance the parent accordingly.
   *
   * If the parent (or grandparent) is an element, return to it.
   * We expect the next call to be to endElement of that.
   *
   * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
   */
  @Override
  public void endElement(
      String uri,
      String localName,
      String qName)
      throws SAXException
  {
    final QName elemQName = new QName(uri, localName);

    try {
      // Do stuff.
    } catch (Exception e) {
      throw new SAXException("Error occurred while ending element " + elemQName + "; traversed path was " + getElementsTraversedAsString(), e);
    }
  }

  @Override
  public void endDocument() throws SAXException {
  }

  /**
   * This can be used to generate an external representation of the
   * internal state tree for visualization (and debugging) purposes.
   *
   * @return The root node of the tree built internally to represent
   *         the XML Document against its XML Schema.
   */
  StateMachineTreeWithState getRootOfInternalTree() {
    return rootStateNode;
  }

  private List<PathSegment> find(
      DocumentPathNode startNode,
      StateMachineTreeWithState tree,
      QName elemQName) {

    if (startNode.getStateMachineNode()
        != currentPosition.stateMachineNode) {

      throw new IllegalStateException("While searching for " + elemQName + ", the DocumentPathNode state machine (" + startNode.getStateMachineNode().getNodeType() + ") does not match the tree node (" + tree.stateMachineNode.getNodeType() + ").");

    } else if (startNode.getIteration() != tree.currIteration) {
      throw new IllegalStateException("While searching for " + elemQName + ", the DocumentPathNode iteration (" + startNode.getIteration() + ") was not kept up-to-date with the tree node's iteration (" + tree.currIteration + ").  Current state machine position is " + tree.stateMachineNode.getNodeType());

    } else if (tree
                 .stateMachineNode
                 .getNodeType()
                 .equals(SchemaStateMachineNode.Type.SEQUENCE)
        && (startNode.getIndexOfNextNodeState()
            != tree.currPositionInSeqGroup)) {

      throw new IllegalStateException("While processing a sequence group in search of " + elemQName + ", the current position in the DocumentPathNode (" + startNode.getIndexOfNextNodeState() + ") was not kept up-to-date with the tree node's position in the sequence group (" + tree.currPositionInSeqGroup + ").");

    } else if (tree.stateMachineNode.getMaxOccurs() > tree.currIteration) {

      throw new IllegalStateException("While searching for " + elemQName + " found that a node of type " + tree.stateMachineNode.getNodeType() + " had more iterations in the tree (" + tree.currIteration + ") than were the maximum allowed for the state machine node (" + tree.stateMachineNode.getMaxOccurs() + ").");

    } else if (tree.stateMachineNode.getMaxOccurs() == tree.currIteration) {
      /* We already traversed this node the maximum number of times.
       * This path cannot be followed.
       */
      return null;
    }

    final SchemaStateMachineNode state = currentPosition.stateMachineNode;

    // If this is a group, confirm it has children.
    if ( !state.getNodeType().equals(SchemaStateMachineNode.Type.ELEMENT)
        && !state.getNodeType().equals(SchemaStateMachineNode.Type.ANY) ) {

      if (( state.getPossibleNextStates() == null)
          || state.getPossibleNextStates().isEmpty()) {

        throw new IllegalStateException("Group " + state.getNodeType() + " has no children.  Found when processing " + elemQName);

      } else if (tree.children == null) {
        throw new IllegalStateException("StateMachineTreeWithState node represents a " + state.getNodeType() + ", but has no children.  Found when searching for " + elemQName);
      }

    }

    List<PathSegment> choices = null;

    switch (state.getNodeType()) {
    case ELEMENT:
      {
        if (state.getElement().getQName().equals(elemQName)
            && startNode.getIteration() < state.getMaxOccurs()) {

          choices = new ArrayList<PathSegment>(1);
          choices.add( createPathSegment(startNode) );
        }
      }
      break;

    case SEQUENCE:
      {
        // Find the next one in the sequence that matches.
        int position = tree.currPositionInSeqGroup;
        if (position < 0) {
          // Let's just do ourselves a favor and add in all the children now.
          if ( !tree.children.isEmpty() ) {
            throw new IllegalStateException("When searching for " + elemQName + ", reached a sequence group with a negative position but with children defined.");
          }

          for (SchemaStateMachineNode nextState : state.getPossibleNextStates()) {
            tree.children.add( createTreeNode(tree, nextState) );
          }

          position = 0;
        }

        for (int stateIndex = position;
            stateIndex < tree.children.size();
            ++stateIndex) {

          // Process child.
          final StateMachineTreeWithState nextTree =
              tree.children.get(stateIndex);

          final SchemaStateMachineNode nextState = nextTree.stateMachineNode;

          final DocumentPathNode nextPath =
              createDocumentPathNode(startNode, nextState);
          nextPath.setIteration(nextTree.currIteration);

          /* Both the tree node's and the document path node's state machine
           * nodes should point to the same state machine node in memory.
           */
          if ((nextTree.stateMachineNode != nextState)
              || (nextPath.getStateMachineNode() != nextState)) {
            throw new IllegalStateException("The expected state machine node (" + nextState.getNodeType() + ") does not match either the tree node (" + nextTree.stateMachineNode.getNodeType() + ") or the next path (" + nextPath.getStateMachineNode().getNodeType() + ") when searching for " + elemQName);

          } else if (nextTree.currIteration >= nextState.getMaxOccurs()) {
            throw new IllegalStateException("Reached a sequence group when searching for " + elemQName + " whose iteration at the current position (" + nextTree.currIteration + ") was already maxed out (" + nextState.getMaxOccurs() + ").  Was at position " + stateIndex + "; tree node's starting position was " + tree.currPositionInSeqGroup);
          }

          final List<PathSegment> seqPaths =
              find(nextPath, nextTree, elemQName);

          if (seqPaths != null) {
            for (PathSegment seqPath : seqPaths) {
              seqPath.prepend(startNode, stateIndex);
            }

            // nextPath was cloned by all path segments, so it can be recycled.
            recyclePathNode(nextPath);

            if (choices == null) {
              choices = seqPaths;
            } else {
              choices.addAll(seqPaths);
            }
          }

          if (nextTree.currIteration
                < nextTree.stateMachineNode.getMinOccurs()) {

            /* If we have not traversed this node in the sequence the minimum
             * number of times, we cannot advance to the next node in the
             * sequence.
             */
            break;
          }
        }

        break;
      }

    case ALL:
    case SUBSTITUTION_GROUP:
    case CHOICE:
      {
        /* All groups only contain elements.  Find one that matches.
         * The max-occurrence check will confirm it wasn't already selected.
         *
         * Choice groups may have multiple paths through its children
         * which are valid.  In addition, a wild card ("any" element)
         * may be a child of any group, thus creating another decision
         * point.
         */
        for (int stateIndex = 0;
            stateIndex < state.getPossibleNextStates().size();
            ++stateIndex) {

          final SchemaStateMachineNode nextState =
              state.getPossibleNextStates().get(stateIndex);

          if (state.getNodeType().equals(SchemaStateMachineNode.Type.ALL)
              && !nextState
                   .getNodeType()
                   .equals(SchemaStateMachineNode.Type.ELEMENT)
              && !nextState
                   .getNodeType()
                   .equals(SchemaStateMachineNode.Type.ANY)
              && !nextState
                   .getNodeType()
                   .equals(SchemaStateMachineNode.Type.SUBSTITUTION_GROUP)) {

            throw new IllegalStateException("While searching for " + elemQName + ", encountered an All group which contained a child of type " + nextState.getNodeType() + '.');
          }

          final DocumentPathNode nextPath =
              createDocumentPathNode(startNode, nextState);

          StateMachineTreeWithState nextTree = null;

          if (tree.children.size() < stateIndex) {
            throw new IllegalStateException("In group of type " + state.getNodeType() + " when searching for " + elemQName + ", StateMachineTreeWithState contained fewer children (" + tree.children.size() + ") than the next possible state index, " + stateIndex);

          } else if (tree.children.size() == stateIndex) {
            nextTree = createTreeNode(tree, nextState);
            tree.children.add(nextTree);

          } else {
            nextTree = tree.children.get(stateIndex);
          }

          /* At this stage, we are only collecting possible paths to follow.
           * Likewise, we do not want to increment the iteration number yet
           * (or have any other side effects on the tree).
           */
          nextPath.setIteration(nextTree.currIteration);

          /* Both the tree node's and the document path node's state machine
           * nodes should point to the same state machine node in memory.
           */
          if ((nextTree.stateMachineNode != nextState)
              || (nextPath.getStateMachineNode() != nextState)) {
            throw new IllegalStateException("The expected state machine node (" + nextState.getNodeType() + ") does not match either the tree node (" + nextTree.stateMachineNode.getNodeType() + ") or the next path (" + nextPath.getStateMachineNode().getNodeType() + ") when searching for " + elemQName);
          }

          final List<PathSegment> choicePaths =
              find(nextPath, nextTree, elemQName);

          if (choicePaths != null) {
            for (PathSegment choicePath : choicePaths) {
              choicePath.prepend(startNode, stateIndex);
            }

            // nextPath was cloned by all path segments, so it can be recycled.
            recyclePathNode(nextPath);

            if (choices == null) {
              choices = choicePaths;
            } else {
              choices.addAll(choicePaths);
            }
          }
        }

        break;
      }
    case ANY:
      {
        /* If the XmlSchemaAny namespace and processing rules
         * apply, this element matches.  False otherwise.
         */
        if (traversedElements.size() < 2) {
          throw new IllegalStateException("Reached a wildcard element while searching for " + elemQName + ", but we've only seen " + traversedElements.size() + " element(s)!");
        }

        final XmlSchemaAny any = state.getAny();

        if (any.getNamespace() == null) {
          throw new IllegalStateException("The XmlSchemaAny element traversed when searching for " + elemQName + " does not have a namespace!");
        }

        boolean needTargetNamespace = false;
        boolean matches = false;

        List<String> validNamespaces = null;

        if ( any.getNamespace().equals("##any") ) {
          // Any namespace is valid.  This matches.
          matches = true;

        } else if ( any.getNamespace().equals("##other") ) {
          needTargetNamespace = true;
          validNamespaces = new ArrayList<String>(1);

        } else {
          final String[] namespaces = any.getNamespace().trim().split(" ");
          validNamespaces = new ArrayList<String>(namespaces.length);
          for (String namespace : namespaces) {
            if (namespace.equals("##targetNamespace")) {
              needTargetNamespace = true;

            } else if (namespace.equals("##local")
                && (elemQName.getNamespaceURI() == null)) {

              matches = true;

            } else {
              validNamespaces.add(namespace);
            }
          }
        }

        if (!matches) {
          /* At this time, it is not possible to determine the XmlSchemaAny's
           * original target namespace without knowing the original element
           * that owned it.  Likewise, unless the XmlSchemaAny's namespace is
           * an actual namespace, or ##any, or ##local, there is no way to
           * validate it.
           *
           * The work-around is to walk upwards through the tree and find
           * the owning element, then use its namespace as the target
           * namespace.
           */
          if (needTargetNamespace) {
            StateMachineTreeWithState iter = tree;
            while ( !iter
                      .stateMachineNode
                      .getNodeType()
                      .equals(SchemaStateMachineNode.Type.ELEMENT) ) {

              iter = iter.parent;

              if (iter == null) {
                throw new IllegalStateException("Walking up the StateMachineTreeWithState to determine the target namespace of a wildcard element, and reached the root without finding any elements.  Searching for " + elemQName + '.');
              }
            }

            validNamespaces.add(
              iter.stateMachineNode.getElement().getQName().getNamespaceURI());
          }

          matches = validNamespaces.contains( elemQName.getNamespaceURI() );
        }

        if (matches) {
          choices = new ArrayList<PathSegment>(1);
          choices.add( createPathSegment(startNode) );
        }
      }
      break;
    default:
      throw new IllegalStateException("Unrecognized node type " + state.getNodeType() + " when processing element " + elemQName);
    }

    if (choices == null) {
      recyclePathNode(startNode);
    }
    return choices;
  }

  private DocumentPathNode createDocumentPathNode(
      DocumentPathNode previous,
      SchemaStateMachineNode state) {

    if ((unusedNodePool != null) && !unusedNodePool.isEmpty()) {
      DocumentPathNode node = unusedNodePool.remove(unusedNodePool.size() - 1);
      node.update(previous, state);
      return node;
    } else {
      return new DocumentPathNode(previous, state);
    }
  }

  private void recyclePathNode(DocumentPathNode toReuse) {
    if (unusedNodePool == null) {
      unusedNodePool = new ArrayList<DocumentPathNode>();
    }
    unusedNodePool.add(toReuse);
  }

  private StateMachineTreeWithState createTreeNode(
      StateMachineTreeWithState parent,
      SchemaStateMachineNode node) {

    if ((unusedTreePool == null) || unusedTreePool.isEmpty()) {
      return new StateMachineTreeWithState(parent, node);
    } else {
      StateMachineTreeWithState tree =
          unusedTreePool.remove(unusedTreePool.size() - 1);

      tree.parent = parent;
      tree.stateMachineNode = node;
      tree.children = null;
      tree.currIteration = 0;
      tree.currPositionInSeqGroup = -1;

      return tree;
    }
  }

  private void recycleTreeNode(StateMachineTreeWithState tree) {
    if (unusedTreePool == null) {
      unusedTreePool = new ArrayList<StateMachineTreeWithState>();
    }
    unusedTreePool.add(tree);
  }

  private PathSegment createPathSegment(DocumentPathNode endPathNode) {
    PathSegment segment = null;
    if ((unusedPathSegmentPool != null) && !unusedPathSegmentPool.isEmpty()) {
      segment =
          unusedPathSegmentPool.remove(unusedPathSegmentPool.size() - 1);
      segment.set(endPathNode);

    } else {
      segment = new PathSegment(endPathNode);
    }
    return segment;
  }

  private void recyclePathSegment(PathSegment segment) {
    if (unusedPathSegmentPool == null) {
      unusedPathSegmentPool = new ArrayList<PathSegment>();
    }

    if (segment.getAfterStart() != null) {
      /* All of the nodes starting with afterStart
       * were cloned; we can recycle them.
       */
      for (DocumentPathNode iter = segment.getAfterStart();
          iter != null;
          iter = iter.getNext()) {
  
        recyclePathNode(iter);
      }
    }

    unusedPathSegmentPool.add(segment);
  }

  private String getElementsTraversedAsString() {

    final StringBuilder traversed = new StringBuilder("[");
    if ((traversedElements != null) && !traversedElements.isEmpty()) {
      for (int i = 0; i < traversedElements.size() - 1; ++i) {
        traversed.append( traversedElements.get(i) ).append(" | ");
      }
      traversed.append( traversedElements.get(traversedElements.size() - 1) );
    }
    traversed.append(" ]");

    return traversed.toString();
  }

  private final SchemaStateMachineNode rootNode;
  private DocumentPathNode rootPathNode;
  private StateMachineTreeWithState rootStateNode;

  private StateMachineTreeWithState currentPosition;
  private DocumentPathNode currentPath;

  private List<DocumentPathNode> unusedNodePool;
  private List<StateMachineTreeWithState> unusedTreePool;
  private List<PathSegment> unusedPathSegmentPool;

  private ArrayList<QName> traversedElements;
  private ArrayList<DecisionPoint> decisionPoints;
}
