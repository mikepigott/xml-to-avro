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
import java.util.List;

import javax.xml.namespace.QName;

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

  /**
   * This represents a range of consecutive nodes through the XML Schema,
   * starting with the <code>start</code> and ending with the <code>end</code>.
   * The number of nodes, inclusive, is tracked by <code>length</code>.
   */
  private static class PathSegment {
    PathSegment() {
      start = null;
      end = null;
      length = 0;
    }

    PathSegment(DocumentPathNode start, DocumentPathNode end) {
      this.start = start;
      this.end = end;
    }

    int getLength() {
      if ((length == 0) && (start != end)) {
        for (DocumentPathNode iter = start;
            iter != end;
            iter = iter.getNext()) {
          ++length;
        }
      }
      return length;
    }

    DocumentPathNode getStart() {
      return start;
    }

    DocumentPathNode getEnd() {
      return end;
    }

    void set(DocumentPathNode start, DocumentPathNode end) {
      this.start = start;
      this.end = end;
      this.length = 0;
    }

    private DocumentPathNode start;
    private DocumentPathNode end;
    private int length;
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
   * Creates a new <code>XmlToAvroPathCreator</code> with the root
   * {@link SchemaStateMachineNode} to start from when evaluating documents.
   */
  XmlToAvroPathCreator(SchemaStateMachineNode root) {
    rootNode = root;
    rootPathNode = new DocumentPathNode(root);
    traversedElements = new ArrayList<QName>();
    currentPosition = null;
    decisionPoints = null; // Hopefully there won't be any!
  }

  @Override
  public void startDocument() throws SAXException {
    currentPosition = rootPathNode;
    traversedElements.clear();

    if (decisionPoints != null) {
      decisionPoints.clear();
    }
  }

  @Override
  public void startElement(
      String uri,
      String localName,
      String qName,
      Attributes atts) throws SAXException {

    final QName elemQName = new QName(uri, localName);
    traversedElements.add(elemQName);

    final SchemaStateMachineNode state = currentPosition.getStateMachineNode();


    if (state.getNodeType().equals(SchemaStateMachineNode.Type.ELEMENT)) {

      /* If we are expecting an element, we must match
       * that element.  This is consistent with looking
       * at the root node.
       */

    }

    /* If the element is part of a group, we should
     * determine which element in the group we are
     * working with.
     */

  }

  @Override
  public void characters(
      char[] ch,
      int start,
      int length)
      throws SAXException {
  }

  @Override
  public void endElement(
      String uri,
      String localName,
      String qName)
      throws SAXException
  {
  }

  @Override
  public void endDocument() throws SAXException {
  }

  private List<DocumentPathNode> find(DocumentPathNode startNode, QName elemQName) {
    final SchemaStateMachineNode state = currentPosition.getStateMachineNode();

    List<DocumentPathNode> choices = null;

    switch (state.getNodeType()) {
    case ELEMENT:
      {
        if (state.getElement().getQName().equals(elemQName)
            && startNode.getIteration() <= state.getMaxOccurs()) {

          startNode.setIteration(1); // TODO: Handle loops.

          choices = new ArrayList<DocumentPathNode>(1);
          choices.add(startNode);
        }
      }
      break;

    case SUBSTITUTION_GROUP:
      // Find one that matches.
      break;
    case ALL:
      // Find one that matches, and make sure it wasn't already selected.
      break;
    case SEQUENCE:
      // Find the next one in the sequence that matches.
      break;
    case CHOICE:
      {
        if (( state.getPossibleNextStates() == null)
            || state.getPossibleNextStates().isEmpty()) {

          throw new IllegalStateException("Group " + state.getNodeType() + " has no children.  Found when processing " + elemQName);
        }

        /* Choice groups may have multiple paths through its children
         * which are valid.  In addition, a wild card ("any" element)
         * may be a child of any group, thus creating another decision
         * point.
         */

        for (SchemaStateMachineNode nextState : state.getPossibleNextStates()) {
          DocumentPathNode nextPath = createDocumentPathNode(startNode, nextState);
          List<DocumentPathNode> choicePaths = find(nextPath, elemQName);
          if (choicePaths != null) {
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
      /* If the XmlSchemaAny namespace and processing rules
       * apply, this element matches.  False otherwise.
       */
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

  private final SchemaStateMachineNode rootNode;

  private DocumentPathNode currentPosition;
  private DocumentPathNode rootPathNode;
  private DocumentPathNode oldNextNode;

  private List<DocumentPathNode> unusedNodePool;

  private ArrayList<QName> traversedElements;
  private ArrayList<DecisionPoint> decisionPoints;
}
