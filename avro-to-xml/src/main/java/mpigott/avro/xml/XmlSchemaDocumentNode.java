package mpigott.avro.xml;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

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
final class XmlSchemaDocumentNode<U> {
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

  void addVisitor(XmlSchemaPathNode path) {
    if (path.getDocumentNode() != this) {
      throw new IllegalArgumentException("Path node must have this XmlSchemaDocumentNode as its document node.");
    }

    switch( path.getDirection() ) {
    case CHILD:
    case SIBLING:
      break;
    default:
      throw new IllegalArgumentException("Only child and sibling paths may be visitors of an XmlSchemaDocumentNode, not a " + path.getDirection() + " path.");
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

    if (visitors.size() != children.size()) {
      throw new IllegalStateException("The number of visitors (" + visitors.size() + ") does not match the number of occurrences (" + children.size() + ").");
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
    children.remove(visitorIndex);
    return true;
  }

  int getIteration() {
    if ((children != null) && (children.size() != visitors.size())) {
      throw new IllegalStateException("The number of occurrences (" + children.size() + ") is not equal to the number of visitors (" + visitors.size() + ").");
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

  private XmlSchemaStateMachineNode stateMachineNode;
  private XmlSchemaDocumentNode parent;
  private List<SortedMap<Integer, XmlSchemaDocumentNode<U>>> children;
  private List<XmlSchemaPathNode> visitors;
  private boolean receivedContent;
  private U userDefinedContent;
}