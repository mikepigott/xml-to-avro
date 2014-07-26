package mpigott.avro.xml;

import java.util.ArrayList;
import java.util.List;

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
class XmlSchemaDocumentNode {
  XmlSchemaDocumentNode(SchemaStateMachineNode stateMachineNode) {
    this.stateMachineNode = stateMachineNode;

    if ((this.stateMachineNode.getPossibleNextStates() == null)
        || this.stateMachineNode.getPossibleNextStates().isEmpty()) {
      this.children = null;

    } else {
      this.children =
          new ArrayList<XmlSchemaDocumentNode>(
              this.stateMachineNode.getPossibleNextStates().size() );
    }

    this.parent = null;
    this.currIteration = 0;
    this.currPositionInSeqGroup = -1;
  }

  XmlSchemaDocumentNode(
      XmlSchemaDocumentNode parent,
      SchemaStateMachineNode stateMachineNode) {

    this(stateMachineNode);
    this.parent = parent;
  }

  SchemaStateMachineNode stateMachineNode;
  XmlSchemaDocumentNode parent;
  List<XmlSchemaDocumentNode> children;

  int currIteration;
  int currPositionInSeqGroup;
}