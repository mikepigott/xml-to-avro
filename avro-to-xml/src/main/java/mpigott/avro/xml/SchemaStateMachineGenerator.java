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

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;

/**
 * Generates a state machine from an {@link XmlSchema} and
 * a {@link Schema} for walking XML documents matching both.
 *
 * @author  Mike Pigott
 */
final class SchemaStateMachineGenerator implements XmlSchemaVisitor {

  /**
   *  Creates a <code>SchemaStateMachineGenerator</code> with the
   *  Avro {@link Schema} we will be converting XML documents to.
   */
  SchemaStateMachineGenerator(Schema avroSchema) {
    this.avroSchema = avroSchema;
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

  }

  /**
   * Finishes processing the {@link XmlSchemaElement} in the XML Schema.
   *
   * <ol>
   *   <li>
   *     If this {@link XmlSchemaElement} is not ignored, checks if a
   *     {@link SchemaStateMachineNode} was previously created to represent it.
   *   </li>
   *   <li>
   *     If a {@link SchemaStateMachineNode} does not represent this
   *     {@link XmlSchemaElement}, creates a new one representing it
   *     and its {@link XmlSchemaAttribute}s.
   *   </li>
   *   <li>
   *     Adds the {@link SchemaStateMachineNode} to the list of possible
   *     future states of the previous {@link SchemaStateMachineNode}.
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
   *     Creates a {@link SchemaStateMachineNode}
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

  }

  /**
   * Completes processing the substitution group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {

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
   *     Creates a {@link SchemaStateMachineNode}
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
  }

  /**
   * Completes processing the All group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) {
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
   *     Creates a {@link SchemaStateMachineNode}
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
  }

  /**
   * Finishes processing the Choice group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) {
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
   *     Creates a {@link SchemaStateMachineNode}
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
  }

  /**
   * Finishes processing the Sequence group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(XmlSchemaSequence seq) {
  }

  /**
   * {@link XmlSchemaAny} nodes are skipped in the Avro {@link Schema},
   * but they must be part of the state machine.  Creates a
   * {@link SchemaStateMachineNode} to represent it, and adds it as
   * a possible future state of the previous node.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAny(org.apache.ws.commons.schema.XmlSchemaAny)
   */
  @Override
  public void onVisitAny(XmlSchemaAny any) {
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

  private final Schema avroSchema;
}
