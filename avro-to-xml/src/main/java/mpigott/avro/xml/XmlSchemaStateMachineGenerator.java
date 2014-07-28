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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaUse;

/**
 * Generates a state machine from an {@link XmlSchema} and
 * a {@link Schema} for walking XML documents matching both.
 *
 * @author  Mike Pigott
 */
final class XmlSchemaStateMachineGenerator implements XmlSchemaVisitor {

  private static class StackEntry {
    StackEntry(boolean isIgnored) {
      this.isIgnored = isIgnored;
      this.node = null;
      this.unionOfChildrenTypes = null;
      this.nextNodes = new ArrayList<XmlSchemaStateMachineNode>();
    }

    StackEntry(XmlSchemaStateMachineNode node, boolean isIgnored) {
      this.isIgnored = isIgnored;
      this.node = node;
      this.unionOfChildrenTypes = null;
      this.nextNodes = null;
    }

    final XmlSchemaStateMachineNode node;
    final boolean isIgnored;
    final List<XmlSchemaStateMachineNode> nextNodes;

    Schema unionOfChildrenTypes;
  }

  private static class ElementInfo {
    ElementInfo(Schema schema) {
      elementSchema = schema;
      attributes = new ArrayList<XmlSchemaStateMachineNode.Attribute>();
    }

    void addAttribute(XmlSchemaAttribute attr, XmlSchemaTypeInfo attrType) {
      attributes.add( new XmlSchemaStateMachineNode.Attribute(attr, attrType) );
    }

    final List<XmlSchemaStateMachineNode.Attribute> attributes;
    final Schema elementSchema;
  }

  /**
   *  Creates a <code>SchemaStateMachineGenerator</code> with the
   *  Avro {@link Schema} we will be transferring to or from an
   *  XML document.
   *
   *  <p>
   *  If Avro documents will be written (<code>xmlIsWritten</code> is false),
   *  the Avro schema does not need to strictly conform to the XML schema.
   *  If only certain elements are needed, one can make the schema an ARRAY
   *  of UNION of RECORDs/MAPs of the elements to read.  If not all attributes
   *  or children are needed, those fields can be removed from the
   *  corresponding Avro RECORDs.
   *  </p>
   *  <p>
   *  However, if an XML document will be written (<code>xmlIsWritten</code> is
   *  true), the Avro schema must strictly conform to the XML Schema.  An
   *  {@link IllegalStateException} will be thrown if a node is reached in the
   *  Avro {@link Schema} that does not conform to the corresponding node in
   *  the {@link org.apache.ws.commons.schema.XmlSchema}.
   *  </p>
   *
   * @param avroSchema The Avro Schema to convert the XML from/to.
   *
   * @param xmlIsWritten Whether the generated state machine will
   *                     be used to write XML (<code>true</code>)
   *                     or read XML (<code>false</code>).
   *
   * @throws NullPointerException if <code>avroSchema</code>
   *                              is <code>null</code>.
   *
   * @throws IllegalArgumentException if <code>avroSchema</code> does not
   *                                  conform to the requirements.
   */
  XmlSchemaStateMachineGenerator(Schema avroSchema, boolean xmlIsWritten) {
    this.avroSchema = avroSchema;
    this.xmlIsWritten = xmlIsWritten;

    startNode = null;
    validNextElements = null;

    conversionCache = new HashMap<Schema.Type, Set<Schema.Type>>();
    elements = new HashMap<QName, ElementInfo>();
    elementNodes = new HashMap<QName, XmlSchemaStateMachineNode>();
    stack = new ArrayList<StackEntry>();

    if ( avroSchema.getType().equals(Schema.Type.ARRAY) ) {
      // ARRAY of UNION of RECORDs/MAPs is not valid when writing XML.
      if (xmlIsWritten) {
        throw new IllegalArgumentException("The Avro Schema cannot be an ARRAY of UNION of MAPs/RECORDs when writing XML; it must conform to the corresponding XML schema.");
      }

      /* The user is only looking to retrieve specific elements from the XML
       * document.  Likewise, the next valid elements are only the ones in
       * that list.
       *
       * (The expected format is Array<Union<Type>>)
       */
      if ( !avroSchema.getElementType().getType().equals(Schema.Type.UNION) ) {
        throw new IllegalArgumentException("If retrieving only a subset of elements in the document, the Avro Schema must be an ARRAY of UNION of those types, not an Array of " + avroSchema.getElementType().getType());
      }

      // Confirm all of the elements in the UNION are either RECORDs or MAPs.
      verifyIsUnionOfMapsAndRecords( avroSchema.getElementType() );

      validNextElements = avroSchema.getElementType().getTypes();

    } else if ( avroSchema.getType().equals(Schema.Type.UNION) ) {
      /* It is possible for the root element to actually be the root of a
       * substitution group.  If this happens, the root element could be
       * one of many different record types.
       *
       * This can only be valid if the schema is a union of records.
       */
      verifyIsUnionOfMapsAndRecords(avroSchema);

      validNextElements = avroSchema.getTypes();

    } else if ( avroSchema.getType().equals(Schema.Type.RECORD)
        || avroSchema.getType().equals(Schema.Type.MAP) ) {
      // This is a definition of the root element.
      validNextElements = new ArrayList<Schema>(1);
      validNextElements.add(avroSchema);

    } else {
      throw new IllegalArgumentException("The Avro Schema must be one of the following types: RECORD, MAP, UNION of RECORDs/MAPs, or ARRAY of UNION of RECORDs/MAPs.");
    }
  }

  XmlSchemaStateMachineNode getStartNode() {
    return startNode;
  }

  /**
   * Processes an {@link XmlSchemaElement} in the XML Schema.
   *
   * <ol>
   *   <li>
   *     Confirms the {@link XmlSchemaElement} matches an Avro
   *     record at the same place in the Avro {@link Schema}.
   *     If not, this element is ignored.
   *   </li>
   *   <li>
   *     Confirms the {@link XmlSchemaTypeInfo} of the element is consistent
   *     with what is expected from the {@link Schema} record.  If not, throws
   *     an {@link IllegalStateException}.
   *   </li>
   * </ol>
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onEnterElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    Schema elemSchema = null;
    if (validNextElements != null) {
      for (Schema possibleSchema : validNextElements) {
        Schema valueType = possibleSchema;
        if ( possibleSchema.getType().equals(Schema.Type.MAP) ) {
          valueType = possibleSchema.getValueType();
        }
  
        if (!valueType.getType().equals(Schema.Type.RECORD)) {
          // The map must have a value type of record, or it is invalid.
          throw new IllegalStateException("MAPs in Avro Schemas for XML documents must have a value type of RECORD, not " + valueType.getType());
        }
  
        if (valueType.getName().equals( element.getName() )) {
          // Confirm the namespaces match.
          String ns = element.getQName().getNamespaceURI();
          if ((ns != null) && !ns.isEmpty()) {
            try {
              if (!Utils.getAvroNamespaceFor(ns).equals(
                    valueType.getNamespace()))
              {
                // Namespaces do not mach. Try the next schema.
                continue;
              }
            } catch (URISyntaxException e) {
              throw new IllegalStateException("Element \"" + element.getQName() + "\" has a namespace that is not a valid URI", e);
            }
          }
  
          // We found the schema!
          elemSchema = possibleSchema;
          break;
        }
      }
    }

    if (xmlIsWritten && (elemSchema == null)) {
      throw new IllegalStateException("Element \"" + element.getQName() + "\" does not have a corresponding Avro schema.  One is needed when writing XML.");
    }

    final StackEntry entry = new StackEntry(elemSchema == null);

    if (!previouslyVisited) {
      elements.put(element.getQName(), new ElementInfo(elemSchema));
    }

    if (elemSchema != null) {
      /* Child elements are in a field under the same name as the element.
       *
       * In the Avro schema, they may be NULL (no children), a
       * primitive type, or an ARRAY of UNION of MAPs and RECORDs.
       */
      Schema valueType = elemSchema;
      if (elemSchema.getType().equals(Schema.Type.MAP)) {
        valueType = elemSchema.getValueType();
      }

      Schema.Field childrenField = valueType.getField( element.getName() );

      /* If the element has no children, a NULL placeholder is used instead.
       * Likewise, if the children field is null, it means the children have
       * been removed in order to be filtered out. 
       */
      if (xmlIsWritten && (childrenField == null)) {
        throw new IllegalStateException("The children of " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName() + " must exist.  If there are no children, an Avro NULL placeholder is required.");
      }

      if (childrenField != null) {
        final Schema childrenSchema = childrenField.schema();
        switch (childrenSchema.getType()) {
        case ARRAY:
          {
            // All group types are ARRAY of UNION of MAP/RECORD.
            if ( !childrenSchema.getElementType().getType().equals(Schema.Type.UNION) ) {
              throw new IllegalStateException("If the children of " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName() + " are in a group, the corresponding Avro Schema MUST BE an ARRAY of UNION of MAPs/RECORDs, not " + childrenSchema.getElementType().getType());
            }

            verifyIsUnionOfMapsAndRecords( childrenSchema.getElementType() );

            entry.unionOfChildrenTypes = childrenSchema.getElementType();
          }
          break;
        case BOOLEAN:
        case BYTES:
        case DOUBLE:
        case ENUM:
        case FLOAT:
        case INT:
        case LONG:
        case STRING:
          {
            if ( !confirmEquivalent(typeInfo, childrenSchema) ) {
              throw new IllegalStateException("Cannot convert between " + typeInfo + " and " + childrenSchema + " for simple content of " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName());
            }
          }
          break;
        case NULL:
          // There are no children, so no further types are valid.
          break;
        default:
          throw new IllegalStateException("Children of element " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName() + " must be either an ARRAY of UNION of MAP/RECORD or a primitive type, not " + childrenSchema.getType());
        }
      }
    }

    if (entry.unionOfChildrenTypes == null) {
      /* If the root schema is an ARRAY of UNION, then the next valid
       * element will be one of its entries.  Otherwise, there are no
       * next valid entries.
       *
       * We want to push that on the stack for when we exit children
       * of the current element.
       */
      if ( avroSchema.getType().equals(Schema.Type.ARRAY) ) {
        validNextElements = avroSchema.getElementType().getTypes();
        entry.unionOfChildrenTypes = avroSchema.getElementType();
      } else {
        validNextElements = null;
      }

    } else {
      validNextElements = entry.unionOfChildrenTypes.getTypes();
    }

    stack.add(entry);
  }

  /**
   * Finishes processing the {@link XmlSchemaElement} in the XML Schema.
   *
   * <ol>
   *   <li>
   *     If this {@link XmlSchemaElement} is not ignored, checks if a
   *     {@link XmlSchemaStateMachineNode} was previously created to represent it.
   *   </li>
   *   <li>
   *     If a {@link XmlSchemaStateMachineNode} does not represent this
   *     {@link XmlSchemaElement}, creates a new one representing it
   *     and its {@link XmlSchemaAttribute}s.
   *   </li>
   *   <li>
   *     Adds the {@link XmlSchemaStateMachineNode} to the list of possible
   *     future states of the previous {@link XmlSchemaStateMachineNode}.
   *   </li>
   * </ol>
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onExitElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Stack is empty before exiting element " + element.getQName());
    }

    StackEntry entry = stack.remove(stack.size() - 1);

    if (entry.node != null) {
      throw new IllegalStateException("Exiting element " + element.getQName() + " but found a " + entry.node.getNodeType() + " on the stack.");
    }

    final ElementInfo elemInfo = elements.get( element.getQName() );

    XmlSchemaStateMachineNode node = elementNodes.get( element.getQName() );
    if (node == null) {
      node =
        new XmlSchemaStateMachineNode(
            element,
            elemInfo.attributes,
            typeInfo,
            elemInfo.elementSchema);

      elementNodes.put(element.getQName(), node);
    }

    if ( !entry.nextNodes.isEmpty() ) {
      node.addPossibleNextStates(entry.nextNodes);
    }

    if ( stack.isEmpty() ) {
      // This is the root node; we're done!
      startNode = node;

    } else {
      // Attach ourselves to the previous group.
      StackEntry parent = stack.get(stack.size() - 1);
      if (parent.node == null) {
        throw new IllegalStateException("The parent of element " + element.getQName() + " is another element.");
      }

      parent.node.addPossibleNextState(node);

      if (parent.unionOfChildrenTypes == null) {
        validNextElements = null;
      } else {
        validNextElements = parent.unionOfChildrenTypes.getTypes();
      }
    }
  }

  /**
   * Processes the incoming {@link XmlSchemaAttribute}.
   *
   * <ol>
   *   <li>
   *     If the {@link XmlSchemaElement} is not skipped, confirms the
   *     {@link XmlSchemaTypeInfo} of the {@link XmlSchemaAttribute}
   *     is compatible with the Avro schema for the corresponding record.
   *     If the types are not compatible, throws an
   *     {@link IllegalStateException}.
   *   </li>
   *   <li>
   *     Links the attribute to its owning element.
   *   </li>
   * </ol>
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAttribute, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onVisitAttribute(
      XmlSchemaElement element,
      XmlSchemaAttribute attribute,
      XmlSchemaTypeInfo attributeType) {

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Processing attribute " + attribute.getQName() + " for element " + element.getQName() + " but the stack is unexpectedly empty.");
    }

    StackEntry entry = stack.get(stack.size() - 1);

    if (entry.node != null) {
      throw new IllegalStateException("Expected the last node on the stack to be for an element, but it represents a " + entry.node.getNodeType() + " instead.");
    }

    if (entry.isIgnored) {
      // We do not process attributes for ignored elements.
      return;
    }

    final ElementInfo elemInfo = elements.get( element.getQName() );

    Schema valueType = elemInfo.elementSchema;
    if ( valueType.getType().equals(Schema.Type.MAP) ) {
      valueType = valueType.getValueType();
    }

    final Schema.Field attrField = valueType.getField( attribute.getName() );

    if (xmlIsWritten
        && (attrField == null)
        && !attribute.getUse().equals(XmlSchemaUse.OPTIONAL)
        && !attribute.getUse().equals(XmlSchemaUse.PROHIBITED)) {
      throw new IllegalStateException("Element " + element.getQName() + " has a " + attribute.getUse() + " attribute named " + attribute.getQName() + " - when writing to XML, a field in the Avro record must exist.");
    }

    if (attrField != null) {
      Schema attrType = attrField.schema();

      if ( attribute.getUse().equals(XmlSchemaUse.OPTIONAL) 
          && attrType.getType().equals(Schema.Type.UNION) ) {

        /* The XML Schema Attribute may have already been a union, so we
         * need to walk all of the subtypes and pull out the non-NULL ones.
         */
        ArrayList<Schema> subset =
            new ArrayList<Schema>(attrType.getTypes().size() - 1);

        for (Schema unionSchema : attrType.getTypes()) {
          if ( !unionSchema.getType().equals(Schema.Type.NULL) ) {
            subset.add(unionSchema);
          }
        }

        if (subset.size() == 1) {
          attrType = subset.get(0);
        } else {
          attrType = Schema.createUnion(subset);
        }
      }

      if (!confirmEquivalent(attributeType, attrType)) {
        throw new IllegalStateException("Cannot convert element " + element.getQName() + " attribute " + attribute.getQName() + " types between " + attributeType + " and " + attrField.schema());
      }

      elemInfo.addAttribute(attribute, attributeType);
    }
  }

  /**
   * Processes the substitution group.
   *
   * <ol>
   *   <li>
   *     Confirms a substitution group is consistent with
   *     the current position in the Avro {@link Schema}.
   *   </li>
   *   <li>
   *     Creates a {@link XmlSchemaStateMachineNode}
   *     representing the substitution group.
   *   </li>
   *   <li>
   *     Adds the <code>SchemaStateMachineNode</code> to the
   *     set of possible next states for the previous state.
   *   </li>
   * </ol>
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement base) {
    if ((validNextElements == null) || validNextElements.isEmpty()) {
      throw new IllegalStateException("About to enter a substitution group for " + base.getQName() + ", but there are no valid elements to expect.");
    }

    if ( !stack.isEmpty() ) {
      pushGroup(
          XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP,
          base.getMinOccurs(),
          base.getMaxOccurs());

    } else {
      // The root element is part of a substitution group.
      Schema schema = avroSchema;
      if ( avroSchema.getType().equals(Schema.Type.ARRAY) ) {
        /* The schema is an ARRAY of UNION of RECORDs/MAPs, acting as
         * a filter on the XML document nodes to fetch.  Likewise, the
         * valid RECORDs and MAPs to work with are the union of types.
         */
        schema = avroSchema.getElementType();
      }

      if ( schema.getType().equals(Schema.Type.UNION) ) {
        startNode =
            new XmlSchemaStateMachineNode(
                XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP,
                schema,
                base.getMinOccurs(),
                base.getMaxOccurs());

        final StackEntry entry = new StackEntry(startNode, true);
        entry.unionOfChildrenTypes = schema;
        stack.add(entry);

      } else {
        throw new IllegalStateException("The document starts with a substitution group, but the Schema root is of type " + schema.getType() + ", not UNION.");
      }
    }
  }

  /**
   * Completes processing the substitution group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {
    popGroup(XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP);
  }

  /**
   * Processes an All group.
   *
   * <ol>
   *   <li>
   *     Confirms an All group is consistent with the
   *     current position in the Avro {@link Schema}.
   *   </li>
   *   <li>
   *     Creates a {@link XmlSchemaStateMachineNode}
   *     representing the All group.
   *   </li>
   *   <li>
   *     Adds the <code>SchemaStateMachineNode</code> to the
   *     set of possible next states for the previous state.
   *   </li>
   * </ol>
   *
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
   * Completes processing the All group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) {
    popGroup(XmlSchemaStateMachineNode.Type.ALL);
  }

  /**
   * Processes a Choice group.
   *
   * <ol>
   *   <li>
   *     Confirms a Choice group is consistent with the
   *     current position in the Avro {@link Schema}.
   *   </li>
   *   <li>
   *     Creates a {@link XmlSchemaStateMachineNode}
   *     representing the Choice group.
   *   </li>
   *   <li>
   *     Adds the <code>SchemaStateMachineNode</code> to the
   *     set of possible next states for the previous state.
   *   </li>
   * </ol>
   *
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
   * Finishes processing the Choice group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) {
    popGroup(XmlSchemaStateMachineNode.Type.CHOICE);
  }

  /**
   * Processes a Sequence group. 
   *
   * <ol>
   *   <li>
   *     Confirms a Sequence group is consistent with
   *     the current position in the Avro {@link Schema}.
   *   </li>
   *   <li>
   *     Creates a {@link XmlSchemaStateMachineNode}
   *     representing the Sequence group.
   *   </li>
   *   <li>
   *     Adds the <code>SchemaStateMachineNode</code> to the
   *     set of possible next states for the previous state.
   *   </li>
   * </ol>
   *
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
   * Finishes processing the Sequence group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(XmlSchemaSequence seq) {
    popGroup(XmlSchemaStateMachineNode.Type.SEQUENCE);
  }

  /**
   * {@link XmlSchemaAny} nodes are skipped in the Avro {@link Schema},
   * but they must be part of the state machine.  Creates a
   * {@link XmlSchemaStateMachineNode} to represent it, and adds it as
   * a possible future state of the previous node.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAny(org.apache.ws.commons.schema.XmlSchemaAny)
   */
  @Override
  public void onVisitAny(XmlSchemaAny any) {
    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Reached an wildcard with no parent!  The stack is empty.");
    }

    final XmlSchemaStateMachineNode node = new XmlSchemaStateMachineNode(any);

    final StackEntry entry = stack.get(stack.size() - 1);

    if (entry.node == null) {
      throw new IllegalStateException("Reached a wildcard with an element for a parent!");
    }

    entry.node.addPossibleNextState(node);
  }

  /**
   * {@link XmlSchemaAnyAttribute}s are not part of
   * the Avro {@link Schema} and thus are ignored.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAnyAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAnyAttribute)
   */
  @Override
  public void onVisitAnyAttribute(
      XmlSchemaElement element,
      XmlSchemaAnyAttribute anyAttr) {
  }

  // Confirms the root-level Schema is a UNION of MAPs, RECORDs, or both.
  private final void verifyIsUnionOfMapsAndRecords(Schema schema) {
    for (Schema unionType : schema.getTypes()) {
      if (!unionType.getType().equals(Schema.Type.RECORD)
          && !unionType.getType().equals(Schema.Type.MAP)) {
        throw new IllegalArgumentException("The Avro Schema may either be a UNION or an ARRAY of UNION, but only if all of the elements in the UNION are of either type RECORD or MAP, not " + unionType.getType());
      } else if ( unionType.getType().equals(Schema.Type.MAP)
          && !unionType.getValueType().getType().equals(Schema.Type.RECORD) ) {
        throw new IllegalArgumentException("If the Avro Schema is a UNION of MAPs or an ARRAY of UNION of MAPs, all MAP value types must be RECORD, not " + unionType.getValueType().getType());
      }
    }
  }

  /* Confirms two XML Schema simple types are equivalent.  Supported types are:
   *
   * BOOLEAN
   * BYTES
   * DOUBLE
   * ENUM
   * FLOAT
   * INT
   * LONG
   * STRING
   */
  private boolean confirmEquivalent(
      XmlSchemaTypeInfo xmlType,
      Schema avroType) {

    if ((avroType != null)
        && ((xmlType == null) || (xmlType.getAvroType() == null))) {
      return false;

    } else if ((avroType == null)
        && ((xmlType != null) && (xmlType.getAvroType() != null))) {
      return false;

    } else if ((avroType == null)
        && ((xmlType == null) || (xmlType.getAvroType() == null))) {
      return true;

    }

    if (xmlIsWritten) {
      return confirmEquivalent(avroType, xmlType.getAvroType());
    } else {
      return confirmEquivalent(xmlType.getAvroType(), avroType);
    }
  }

  /* Confirms two XML Schema simple types are equivalent.  Supported types are:
   *
   * BOOLEAN
   * BYTES
   * DOUBLE
   * ENUM
   * FLOAT
   * INT
   * LONG
   * STRING
   */
  private boolean confirmEquivalent(Schema readerType, Schema writerType) {

    if (readerType.getType().equals(Schema.Type.ARRAY)
        && (writerType.getType().equals(Schema.Type.ARRAY))) {
      return confirmEquivalent(readerType.getElementType(), writerType.getElementType());

    } else if (readerType.getType().equals(Schema.Type.UNION)
        && writerType.getType().equals(Schema.Type.UNION)) {

      // O(N^2) cross-examination.
      int numFound = 0;
      for (Schema readerUnionType : writerType.getTypes()) {
        for (Schema writerUnionType : readerType.getTypes()) {
          if ( confirmEquivalent(readerUnionType, writerUnionType) ) {
            ++numFound;
            break;
          }
        }
      }
      if (readerType.getTypes().size() == numFound) {
        // We were able to find equivalents for all of the reader types.
        return true;
      } else {
        // We could not find equivalents for all of the reader types.
        return false;
      }
    }

    if ( conversionCache.containsKey(writerType.getType()) ) {
      return conversionCache.get( writerType.getType() )
                            .contains( readerType.getType() );
    }

    final HashSet<Schema.Type> convertibleFrom = new HashSet<Schema.Type>();
    switch ( writerType.getType() ) {
    case STRING:
      // STRING, BOOLEAN, ENUM, DOUBLE, FLOAT, LONG, INT -> STRING
      convertibleFrom.add(Schema.Type.STRING);
      convertibleFrom.add(Schema.Type.BOOLEAN);
      convertibleFrom.add(Schema.Type.ENUM);
    case DOUBLE:
      // DOUBLE, FLOAT, LONG, INT -> DOUBLE
      convertibleFrom.add(Schema.Type.DOUBLE);
    case FLOAT:
      // FLOAT, LONG, INT -> FLOAT
      convertibleFrom.add(Schema.Type.FLOAT);
    case LONG:
      // LONG, INT -> LONG
      convertibleFrom.add(Schema.Type.LONG);
    case INT:
      // INT -> INT
      convertibleFrom.add(Schema.Type.INT);
      break;

    case BOOLEAN:
      // BOOLEAN -> BOOLEAN
      convertibleFrom.add(Schema.Type.BOOLEAN);
      break;

    case BYTES:
      // BYTES -> BYTES
      convertibleFrom.add(Schema.Type.BYTES);
      break;

    case ENUM:
      // This one is more complex.
      break;

    default:
      throw new IllegalArgumentException("Cannot confirm the equivalency of a reader of type " + readerType.getType() + " and a writer of type " + writerType.getType());
    }

    if ( !convertibleFrom.isEmpty() ) {
      conversionCache.put(writerType.getType(), convertibleFrom);
      return convertibleFrom.contains( readerType.getType() );
    }

    /* If we're here, it's because the writer is an ENUM.  Confirm
     * the writer elements are a superset of the reader elements.
     */
    if ( readerType.getType().equals(Schema.Type.ENUM) ) {
      final List<String> writerSymbols = writerType.getEnumSymbols();
      final List<String> readerSymbols = readerType.getEnumSymbols();

      for (String readerSymbol : readerSymbols) {
        if ( !writerSymbols.contains(readerSymbol) ) {
          return false;
        }
      }

      return true;
    }

    return false;
  }

  private void pushGroup(
      XmlSchemaStateMachineNode.Type groupType,
      long minOccurs,
      long maxOccurs) {

    /* NOTE: An XML Schema group may contain only an Any element, and thus a
     *       corresponding Avro Schema with no valid children at this stage
     *       would still be fully conformant.  Likewise, having no valid next
     *       elements is NOT an indicator that this Avro Schema is invalid.
     */

    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Attempted to create a(n) " + groupType + " group with no parent - the stack is empty!");
    }

    final StackEntry parent = stack.get(stack.size() - 1);
    final XmlSchemaStateMachineNode node =
        new XmlSchemaStateMachineNode(
            groupType,
            parent.unionOfChildrenTypes,
            minOccurs,
            maxOccurs);

    if (parent.node != null) {
      // Parent is another group.
      parent.node.addPossibleNextState(node);
    } else {
      // Parent is an element.
      parent.nextNodes.add(node);
    }

    StackEntry entry = new StackEntry(node, parent.isIgnored);
    entry.unionOfChildrenTypes = parent.unionOfChildrenTypes;

    stack.add(entry);
  }

  private void popGroup(XmlSchemaStateMachineNode.Type groupType) {
    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Exiting an " + groupType + " group, but the stack is empty!");
    }

    final StackEntry entry = stack.remove(stack.size() - 1);

    if (entry.node == null) {
      throw new IllegalStateException("Exiting a(n) " + groupType + " group, but an element was on the stack instead!");

    } else if (!entry.node.getNodeType().equals(groupType)) {
      throw new IllegalStateException("Exiting a(n) " + groupType + " group, but found a " + entry.node.getNodeType() + " on the stack instead!");
    }

    if ( !stack.isEmpty() ) {
      final StackEntry parent = stack.get(stack.size() - 1);

      if (parent.unionOfChildrenTypes == null) {
        validNextElements = null;
      } else {
        validNextElements = parent.unionOfChildrenTypes.getTypes();
      }
    } else if (!groupType.equals(
                  XmlSchemaStateMachineNode.Type.SUBSTITUTION_GROUP) ) {
      throw new IllegalStateException(groupType + " group had no parent!  The stack is empty after removing it.");
    }

  }

  private final Schema avroSchema;
  private final boolean xmlIsWritten;

  private final Map<QName, ElementInfo> elements;
  private final List<StackEntry> stack;
  private final Map<Schema.Type, Set<Schema.Type>> conversionCache;

  private List<Schema> validNextElements;
  private XmlSchemaStateMachineNode startNode;
  private Map<QName, XmlSchemaStateMachineNode> elementNodes;
}
