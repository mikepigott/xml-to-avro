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

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;

/**
 * Builds a state machine from an
 * {@link org.apache.ws.commons.schema.XmlSchema}.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaStateMachineGenerator implements XmlSchemaVisitor {

  private static class ElementInfo {
    ElementInfo(
        XmlSchemaElement element,
        XmlSchemaTypeInfo typeInfo) {

      this.element = element;
      this.typeInfo = typeInfo;
      this.attributes = new ArrayList<XmlSchemaStateMachineNode.Attribute>();
      this.stateMachineNode = null;
    }

    void addAttribute(XmlSchemaAttribute attr, XmlSchemaTypeInfo attrType) {
      attributes.add( new XmlSchemaStateMachineNode.Attribute(attr, attrType) );
    }

    final List<XmlSchemaStateMachineNode.Attribute> attributes;
    final XmlSchemaTypeInfo typeInfo;
    final XmlSchemaElement element;

    XmlSchemaStateMachineNode stateMachineNode;
  }

  public XmlSchemaStateMachineGenerator() {
    stack = new ArrayList<XmlSchemaStateMachineNode>();
    elementInfoByQName = new HashMap<QName, ElementInfo>();
    startNode = null;
  }

  XmlSchemaStateMachineNode getStartNode() {
    return startNode;
  }

  Map<QName, XmlSchemaStateMachineNode> getStateMachineNodesByQName() {
    final HashMap<QName, XmlSchemaStateMachineNode> nodes =
        new HashMap<QName, XmlSchemaStateMachineNode>();

    for (Map.Entry<QName, ElementInfo> entry : elementInfoByQName.entrySet()) {
      nodes.put(entry.getKey(), entry.getValue().stateMachineNode);
    }

    return nodes;
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onEnterElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    if (!previouslyVisited) {
      /* This is our first encounter of the element.  We do not have the
       * attributes yet, so we cannot create a state machine node for it.
       * However, we will have all of the attributes once onEndAttributes()
       * is called, so we can create an ElementInfo entry for it, and wait
       * until later to create the state machine and add it to the stack.
       */
      final ElementInfo info = new ElementInfo(element, typeInfo);
      elementInfoByQName.put(element.getQName(), info);

    } else {
      /* We have previously encountered this element, which means we have
       * already collected all of the information we needed to build an
       * XmlSchemaStateMachineNode.  Likewise, we can just reference it.
       */
      final ElementInfo elemInfo =
          elementInfoByQName.get( element.getQName() );
      if ((elemInfo == null) || (elemInfo.stateMachineNode == null)) {
        throw new IllegalStateException("Element " + element.getQName() + " was already visited, but we do not have a state machine for it.");
      } else if ( stack.isEmpty() ) {
        throw new IllegalStateException("Element " + element.getQName() + " was previously visited, but there is no parent state machine node to attach it to!");
      }

      stack.get(stack.size() - 1)
           .addPossibleNextState(elemInfo.stateMachineNode);

      stack.add(elemInfo.stateMachineNode);
    }
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onExitElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Exiting " + element.getQName() + ", but the stack is empty.");
    }

    final XmlSchemaStateMachineNode node = stack.remove(stack.size() - 1);
    if (!node.getNodeType().equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalStateException("Exiting element " + element.getQName() + ", but  " + node + " is on the stack.");
    } else if (!node.getElement().getQName().equals(element.getQName())) {
      throw new IllegalStateException("Element " + element.getQName() + " is not the same in-memory copy we received on creation.  Our copy is of a " + node.getElement().getQName());
    }
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAttribute, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onVisitAttribute(XmlSchemaElement element,
      XmlSchemaAttribute attribute, XmlSchemaTypeInfo attributeType) {

    final ElementInfo elemInfo = elementInfoByQName.get(element.getQName());
    if (elemInfo == null) {
      throw new IllegalStateException("No record exists for element " + element.getQName());
    }

    elemInfo.addAttribute(attribute, attributeType);
  }

  @Override
  public void onEndAttributes(
      XmlSchemaElement element,
      XmlSchemaTypeInfo elemTypeInfo) {

    /* The parent of this group is an element
     * that needs to be added to the stack.
     */
    final ElementInfo elemInfo = elementInfoByQName.get(element.getQName());

    if (elemInfo.stateMachineNode != null) {
      throw new IllegalStateException("Parent element " + element.getQName() + " is supposedly undefined, but that entry already has a state machine of " + elemInfo.stateMachineNode);
    }

    elemInfo.stateMachineNode =
        new XmlSchemaStateMachineNode(
            elemInfo.element,
            elemInfo.attributes,
            elemInfo.typeInfo);

    if ( !stack.isEmpty() ) {
      stack.get(stack.size() - 1)
           .addPossibleNextState(elemInfo.stateMachineNode);
    } else {
      // This is the root node.
      startNode = elemInfo.stateMachineNode;
    }

    stack.add(elemInfo.stateMachineNode);

  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement base) {
    if ( stack.isEmpty() ) {
      // The root element is the base of a substitution group.
      startNode =
          new XmlSchemaStateMachineNode(
              XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP,
              base.getMinOccurs(),
              base.getMaxOccurs());
      stack.add(startNode);
    } else {
      pushGroup(
          XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP,
          base.getMinOccurs(),
          base.getMaxOccurs());
    }
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {
    popGroup(XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onEnterAllGroup(XmlSchemaAll all) {
    pushGroup(
        XmlSchemaStateMachineNode.Type.ALL,
        all.getMinOccurs(),
        all.getMaxOccurs());
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) {
    popGroup(XmlSchemaStateMachineNode.Type.ALL);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice choice) {
    pushGroup(
        XmlSchemaStateMachineNode.Type.CHOICE,
        choice.getMinOccurs(),
        choice.getMaxOccurs());
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) {
    popGroup(XmlSchemaStateMachineNode.Type.CHOICE);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onEnterSequenceGroup(XmlSchemaSequence seq) {
    pushGroup(
        XmlSchemaStateMachineNode.Type.SEQUENCE,
        seq.getMinOccurs(),
        seq.getMaxOccurs());
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(XmlSchemaSequence seq) {
    popGroup(XmlSchemaStateMachineNode.Type.SEQUENCE);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAny(org.apache.ws.commons.schema.XmlSchemaAny)
   */
  @Override
  public void onVisitAny(XmlSchemaAny any) {
    final XmlSchemaStateMachineNode node =
        new XmlSchemaStateMachineNode(any);

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Reached an wildcard with no parent!  The stack is empty.");
    }

    stack.get(stack.size() - 1).addPossibleNextState(node);
  }

  /**
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAnyAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAnyAttribute)
   */
  @Override
  public void onVisitAnyAttribute(XmlSchemaElement element,
      XmlSchemaAnyAttribute anyAttr) {

    // Ignored.
  }

  private void pushGroup(
      XmlSchemaStateMachineNode.Type groupType,
      long minOccurs,
      long maxOccurs) {

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Attempted to create a(n) " + groupType + " group with no parent - the stack is empty!");
    }

    final XmlSchemaStateMachineNode node =
        new XmlSchemaStateMachineNode(
            groupType,
            minOccurs,
            maxOccurs);

    stack.get(stack.size() - 1).addPossibleNextState(node);
    stack.add(node);
  }

  private void popGroup(XmlSchemaStateMachineNode.Type groupType) {
    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Exiting an " + groupType + " group, but the stack is empty!");
    }

    final XmlSchemaStateMachineNode node = stack.remove(stack.size() - 1);

    if (!node.getNodeType().equals(groupType)) {
      throw new IllegalStateException("Attempted to pop a " + groupType + " off of the stack, but found a " + node.getNodeType() + " instead!");
    }

    if (!groupType.equals(XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP)
        && stack.isEmpty()) {
      throw new IllegalStateException("Popped a group of type " + groupType + " only to find it did not have a parent.");
    }
  }

  private List<XmlSchemaStateMachineNode> stack;
  private XmlSchemaStateMachineNode startNode;
  private Map<QName, ElementInfo> elementInfoByQName;
}
