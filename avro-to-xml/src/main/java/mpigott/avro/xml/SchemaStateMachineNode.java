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
 *   <li>
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
    SEQUENCE
  }

  /**
   * Constructs a new <code>SchemaStateMachineNode</code> for a group.
   *
   * @param nodeType The type of the group node ({@link Type#SUBSTITUTION_GROUP},
   *                 {@link Type#ALL}, {@link Type#CHOICE}, or
   *                 {@link Type#SEQUENCE}).
   *
   * @param minOccurs The minimum number of occurrences of this group.
   * @param maxOccurs The maximum number of occurrences of this group.
   *
   * @throws IllegalArgumentException if this constructor is used to
   *                                  define an {@link Type#ELEMENT}.
   */
  public SchemaStateMachineNode(Type nodeType, long minOccurs, long maxOccurs) {
    if (nodeType.equals(Type.ELEMENT)) {
      throw new IllegalArgumentException("This constructor cannot be used for elements.");
    }

    this.nodeType = nodeType;
    this.minOccurs = minOccurs;
    this.maxOccurs = maxOccurs;

    this.element = null;
    this.attributes = null;
    this.typeInfo = null;
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
  public SchemaStateMachineNode(
      XmlSchemaElement elem,
      List<XmlSchemaAttribute> attrs,
      XmlSchemaTypeInfo typeInfo)
  {
    this.nodeType = Type.ELEMENT;
    this.element = elem;
    this.attributes = attrs;
    this.typeInfo = typeInfo;
    this.minOccurs = elem.getMinOccurs();
    this.maxOccurs = elem.getMaxOccurs();
  }

  private Type nodeType;
  private XmlSchemaElement element;
  private List<XmlSchemaAttribute> attributes;
  private XmlSchemaTypeInfo typeInfo;
  private long minOccurs;
  private long maxOccurs;

  private List<SchemaStateMachineNode> nextPossibleStates;
}
