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

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;

/**
 * This represents a node in the state
 * machine used when parsing an XML
 * {@link org.w3c.dom.Document} based on its
 * {@link org.apache.ws.commons.schema.XmlSchema}
 * and Avro {@link org.apache.avro.Schema}.
 *
 * <p>
 * A <code>SchemaStateMachineNode</code> represents one of:
 * <ul>
 *   <li>An element, its type, and its attributes</li>
 *   <li>A substitution group</li>
 *   <li>An all group</li>
 *   <li>A choice group</li>
 *   <li>A sequence group</li>
 *   <li>An &lt;any&gt; node.</li>
 * </ul>
 * As a {@link org.w3c.dom.Document} is traversed, the state machine is used
 * to determine how to process the current element.  Two passes will be needed:
 * the first pass will determine the correct path through the document's schema
 * in order to properly parse the elements, and the second traversal will read
 * the elements while following that path. 
 * </p>
 *
 * @author  Mike Pigott
 */
final class SchemaStateMachineNode {

  enum Type {
    ELEMENT,
    SUBSTITUTION_GROUP,
    ALL,
    CHOICE,
    SEQUENCE,
    ANY
  }

  static class Attribute {
    Attribute(XmlSchemaAttribute attribute, XmlSchemaTypeInfo attrType) {
      this.attribute = attribute;
      this.attrType = attrType;
    }

    XmlSchemaAttribute getAttribute() {
      return attribute;
    }

    XmlSchemaTypeInfo getType() {
      return attrType;
    }

    private final XmlSchemaAttribute attribute;
    private final XmlSchemaTypeInfo attrType;
  }

  /**
   * Constructs a new <code>SchemaStateMachineNode</code> for a group.
   *
   * @param nodeType The type of the group node ({@link Type#ALL},
   *                 {@link Type#SUBSTITUTION_GROUP}, {@link Type#CHOICE},
   *                 {@link Type#SEQUENCE}, or {@link Type#ANY}).
   *
   * @param minOccurs The minimum number of occurrences of this group.
   * @param maxOccurs The maximum number of occurrences of this group.
   *
   * @throws IllegalArgumentException if this constructor is used to
   *                                  define an {@link Type#ELEMENT}.
   */
  SchemaStateMachineNode(Type nodeType, long minOccurs, long maxOccurs) {
    if (nodeType.equals(Type.ELEMENT)) {
      throw new IllegalArgumentException("This constructor cannot be used for elements.");
    }

    this.nodeType = nodeType;
    this.minOccurs = minOccurs;
    this.maxOccurs = maxOccurs;

    this.element = null;
    this.attributes = null;
    this.typeInfo = null;

    this.possibleNextStates = new ArrayList<SchemaStateMachineNode>();
  }

  /**
   * Constructs a new <code>SchemaStateMachineNode</code> for an element.
   *
   * @param elem The {@link XmlSchemaElement} this node represents.
   *
   * @param attrs The {@link XmlSchemaAttribute} contained by this element.
   *              An empty {@link List} or <code>null</code> if none.
   *
   * @param typeInfo The type information, if the element has simple content.
   *                 <code>null</code> if not.
   */
  SchemaStateMachineNode(
      XmlSchemaElement elem,
      List<Attribute> attrs,
      XmlSchemaTypeInfo typeInfo)
  {
    this.nodeType = Type.ELEMENT;
    this.element = elem;
    this.attributes = attrs;
    this.typeInfo = typeInfo;
    this.minOccurs = elem.getMinOccurs();
    this.maxOccurs = elem.getMaxOccurs();

    this.possibleNextStates = new ArrayList<SchemaStateMachineNode>();
  }

  /**
   * The XML Schema node {@link Type} this
   * <code>SchemaStateMachineNode</code> represents.
   */
  Type getNodeType() {
    return nodeType;
  }

  /**
   * If this <code>SchemaStateMachineNode</code> represents an
   * {@link XmlSchemaElement}, the <code>XmlSchemaElement</code>
   * it represents.
   */
  XmlSchemaElement getElement() {
    return element;
  }

  /**
   * If this <code>SchemaStateMachineNode</code> represents an
   * {@link XmlSchemaElement}, the {@link XmlSchemaTypeInfo}
   * of the element it represents.
   */
  XmlSchemaTypeInfo getElementType() {
    return typeInfo;
  }

  /**
   * If this <code>SchemaStateMachineNode</code> represents an
   * {@link XmlSchemaElement}, the set of {@link Attribute}s
   * associated with the element it represents.
   */
  List<Attribute> getAttributes() {
    return attributes;
  }

  /**
   * The minimum number of times this <code>SchemaStateMachineNode</code>
   * may appear in succession.
   */
  long getMinOccurs() {
    return minOccurs;
  }

  /**
   * The maximum number of times this <code>SchemaStateMachineNode</code>
   * may appear in succession.
   */
  long getMaxOccurs() {
    return maxOccurs;
  }

  /**
   * Adds a state that could follow this <code>SchemaStateMachineNode</code>.
   *
   * @param next A node that could follow this one in the XML document.
   * @return Itself, for chaining.
   */
  SchemaStateMachineNode addPossibleNextState(SchemaStateMachineNode next) {
    possibleNextStates.add(next);
    return this;
  }

  /**
   * Adds the set of possible states that could follow
   * this <code>SchemaStateMachineNode</code>.
   *
   * @param nextStates The set of possible nodes that could
   *                   follow this one in the XML document.
   *
   * @return Itself, for chaining.
   */
  SchemaStateMachineNode addPossibleNextStates(java.util.Collection<SchemaStateMachineNode> nextStates) {
    possibleNextStates.addAll(nextStates);
    return this;
  }

  /**
   * All of the known possible states that could follow this one.
   */
  List<SchemaStateMachineNode> getPossibleNextStates() {
    return possibleNextStates;
  }

  private final Type nodeType;
  private final XmlSchemaElement element;
  private final List<Attribute> attributes;
  private final XmlSchemaTypeInfo typeInfo;
  private final long minOccurs;
  private final long maxOccurs;

  private List<SchemaStateMachineNode> possibleNextStates;
}
