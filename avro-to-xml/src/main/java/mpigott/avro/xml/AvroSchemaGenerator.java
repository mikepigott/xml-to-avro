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
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAnnotation;
import org.apache.ws.commons.schema.XmlSchemaAnnotationItem;
import org.apache.ws.commons.schema.XmlSchemaAny;
import org.apache.ws.commons.schema.XmlSchemaAnyAttribute;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaDocumentation;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaUse;
import org.w3c.dom.NodeList;

/**
 * Generates an Avro schema based on the walked XML Schema.
 *
 * @author  Mike Pigott
 */
final class AvroSchemaGenerator implements XmlSchemaVisitor {

  private static class StackEntry {
    public StackEntry(final QName elementQName, final boolean isSubstitutionGroup) {
      this.elementQName = elementQName;
      this.isSubstitutionGroup = isSubstitutionGroup;
    }

    final QName elementQName;
    final boolean isSubstitutionGroup;
  }

  public AvroSchemaGenerator() {
    root = null;
    stack = new ArrayList<StackEntry>();
    schemasByElement = new HashMap<QName, Schema>();
    substitutionGroups = new HashMap<QName, List<Schema>>();
    fieldsByElement = new HashMap<QName, List<Schema>>();
    attributesByElement = new HashMap<QName, List<Schema.Field>>();
  }

  /**
   * Clears the internal state of the {@link AvroSchemaGenerator}
   * so a new Avro {@link Schema} can be generated from a new
   * {@link org.apache.ws.commons.schema.XmlSchema}.
   */
  public void clear() {
    root = null;
    stack.clear();
    schemasByElement.clear();
    substitutionGroups.clear();
    fieldsByElement.clear();
    attributesByElement.clear();
  }

  /**
   * The generated {@link Schema}, or <code>null</code> if none.
   *
   * @return The generated {@link Schema}, or <code>null</code> if none.
   */
  public Schema getSchema() {
    return root;
  }

  /**
   * If this element was not added previously, registers a new schema for it.
   * If this element was added previously, and it is not part of a substitution
   * group, add it to its parent.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onEnterElement(
      final XmlSchemaElement element,
      final XmlSchemaTypeInfo typeInfo,
      final boolean previouslyVisited) {

    final QName elemQName = element.getQName();
    final StackEntry substGrp = getSubstitutionGroup();

    if (previouslyVisited) {
      if (substGrp == null) {
        /* This element is not part of a substitution group, so
         * it is simply a child of its parent.  Add it as such.
         *
         * If this were a member of a substitution group, then it
         * would have been added to that substitution group the first
         * time it was encountered.  Once the substitution group has
         * been fully processed, all of its elements will be added
         * to the proper parent(s) at once.  Likewise, nothing needs
         * to be done here.
         */
        final Schema schema = schemasByElement.get(elemQName);
        if (schema == null) {
          throw new IllegalStateException("Element \"" + element.getQName() + "\" was previously visited, but has no schema.");
        }

        addSchemaToParent(schema);
      }

      return;
    }

    // If this element is abstract, it is not a member of the substitution group.
    if ((substGrp != null) && element.isAbstract()) {
      return;
    }

    // If documentation is available, makes it the record's documentation.
    final String documentation = getDocumentationFor( element.getAnnotation() );

    // Create the record.
    Schema record = null;
    try {
      record =
          Schema.createRecord(
              elemQName.getLocalPart(),
              documentation,
              Utils.getAvroNamespaceFor( elemQName.getNamespaceURI() ),
              false);

    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Element \"" + elemQName + "\" has an invalid namespace of \"" + elemQName.getNamespaceURI() + "\"", e);
    }

    schemasByElement.put(elemQName, record);

    if (substGrp != null) {
      /* This element is part of a substitution group.
       * It will be added to its parent(s) later.
       */
      List<Schema> substitutionSchemas =
          substitutionGroups.get(substGrp.elementQName);
      if (substitutionSchemas == null) {
        substitutionSchemas = new ArrayList<Schema>();
        substitutionGroups.put(substGrp.elementQName, substitutionSchemas);
      }
      substitutionSchemas.add(record);

    } else if (root == null) {
      // This is the root element!
      root = record;

    } else {
      /* This is not part of a substitution group,
       * and not the root.  Add it to its parent.
       */
      addSchemaToParent(record);
    }

    stack.add( new StackEntry(elemQName, false) );
  }

  /**
   * If this element was not visited previously, retrieves all of the fields
   * associated with the record corresponding to the element, and adds them.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitElement(org.apache.ws.commons.schema.XmlSchemaElement, mpigott.avro.xml.XmlSchemaTypeInfo, boolean)
   */
  @Override
  public void onExitElement(
      XmlSchemaElement element,
      XmlSchemaTypeInfo typeInfo,
      boolean previouslyVisited) {

    if (previouslyVisited) {
      /* No element was added to the stack, so no element should be removed
       * from the stack. This element either has been processed already or
       * will be processed later.
       */
      return;

    } else if ((getSubstitutionGroup() != null) && element.isAbstract()) {

      /* This element is part of a substitution group and was declared
       * abstract, so it will not be a valid child element.
       */
      return;
    }

    final StackEntry entry = pop(element.getQName(), false);

    final Schema record = schemasByElement.get(entry.elementQName);
    if (record == null) {
      throw new IllegalStateException("No schema found for element \"" + entry.elementQName + "\".");
    }

    List<Schema.Field> fields = attributesByElement.get(entry.elementQName);
    if (fields == null) {
      fields = new ArrayList<Schema.Field>(1);
    }

    List<Schema> children = fieldsByElement.get(entry.elementQName);

    // TODO: Handle mixed elements.

    if ((children != null) && !children.isEmpty() && (typeInfo != null) && (typeInfo.getAvroType() != null)) {
      throw new IllegalStateException("Element \"" + entry.elementQName + "\" has both a type (" + typeInfo.getAvroType() + ") and " + children.size() + " child elements.");

    } else if ((children != null) && !children.isEmpty()) {
      final Schema schema = Schema.createArray( Schema.createUnion(children) );
      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              schema,
              "Children of " + entry.elementQName,
              null);
      fields.add(field);

    } else if ((typeInfo != null) && (typeInfo.getAvroType() != null)) {
      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              typeInfo.getAvroType(),
              "Simple type " + typeInfo.getAvroType(),
              null);
      fields.add(field);
    }

    if ( fields.isEmpty() ) {
      // This element is nillable and only needed to exist.
      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              Schema.create(Type.NULL),
              "This element contains no attributes and no children.",
              null);
      fields.add(field);
    }

    record.setFields(fields);

    if (typeInfo != null) {
      record.addProp("xmlSchema", typeInfo.getXmlSchemaAsJson());
    }
  }

  /**
   * Processes the attribute of the provided element,
   * adding it as a field to the corresponding record.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAttribute, mpigott.avro.xml.XmlSchemaTypeInfo)
   */
  @Override
  public void onVisitAttribute(
      XmlSchemaElement element,
      XmlSchemaAttribute attribute,
      XmlSchemaTypeInfo attributeType) {

    if ((getSubstitutionGroup() != null) && element.isAbstract()) {
      // Abstract elements are ignored.
      return;
    }

    if ( attribute.getUse().equals(XmlSchemaUse.PROHIBITED) ) {
      // This attribute is prohibited and cannot be part of the record.
      return;
    }

    final QName attrQName = attribute.getQName();
    final StackEntry entry = getParentElement();

    if (!entry.elementQName.equals(element.getQName())) {
      throw new IllegalStateException("Attribute \"" + attrQName + "\" belongs to element \"" + element.getQName() + "\", but parent element \"" + entry.elementQName + "\" was found on the stack instead.");
    }

    final String documentation =
        getDocumentationFor( attribute.getAnnotation() );

    String defaultValue = attribute.getDefaultValue();
    if (defaultValue == null) {
      defaultValue = attribute.getFixedValue();
    }

    Schema attrSchema = attributeType.getAvroType();

    // Optional types are unions of the real type and null.
    if ( attribute.getUse().equals(XmlSchemaUse.OPTIONAL) ) {
      ArrayList<Schema> unionTypes = new ArrayList<Schema>(2);
      unionTypes.add(attrSchema);
      unionTypes.add( Schema.create(Schema.Type.NULL) );
      attrSchema = Schema.createUnion(unionTypes);
    }

    final Schema.Field attr =
        new Schema.Field(
            attrQName.getLocalPart(),
            attrSchema,
            documentation,
            Utils.createJsonNodeFor(defaultValue, attributeType.getAvroType()));

    attr.addProp("xmlSchema", attributeType.getXmlSchemaAsJson());

    List<Schema.Field> attrs = attributesByElement.get(entry.elementQName);
    if (attrs == null) {
      attrs = new ArrayList<Schema.Field>();
      attributesByElement.put(entry.elementQName, attrs);
    }
    attrs.add(attr);
  }

  /**
   * Adds a new stack entry for this substitution group.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement base) {
    stack.add( new StackEntry(base.getQName(), true) );
  }

  /**
   * Retrieves all of the members of this substitution
   * group and adds them to the parent as children.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSubstitutionGroup(org.apache.ws.commons.schema.XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {
    final StackEntry entry = pop(base.getQName(), true);
    final List<Schema> substitutes = substitutionGroups.get(entry.elementQName);
    if ((substitutes == null) || substitutes.isEmpty()) {
      /* This happens when an abstract element can only
       * be substituted by other abstract elements.
       */
      return;
    }

    StackEntry parent = getParentElement();
    List<Schema> siblings = fieldsByElement.get(parent.elementQName);
    if (siblings == null) {
      siblings = new ArrayList<Schema>( substitutes.size() );
    }
    siblings.addAll(substitutes);
  }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onEnterAllGroup(XmlSchemaAll all) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitAllGroup(org.apache.ws.commons.schema.XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice choice) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitChoiceGroup(org.apache.ws.commons.schema.XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onEnterSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onEnterSequenceGroup(final XmlSchemaSequence seq) {  }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onExitSequenceGroup(org.apache.ws.commons.schema.XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(final XmlSchemaSequence seq) { }

  /**
   * Avro schemas do not have support for an
   * "anything" type. This method is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAny(org.apache.ws.commons.schema.XmlSchemaAny)
   */
  @Override
  public void onVisitAny(XmlSchemaAny any) { }

  /**
   * Avro schemas do not have support for an
   * "anything" type. This method is a no-op.
   *
   * @see mpigott.avro.xml.XmlSchemaVisitor#onVisitAnyAttribute(org.apache.ws.commons.schema.XmlSchemaElement, org.apache.ws.commons.schema.XmlSchemaAnyAttribute)
   */
  @Override
  public void onVisitAnyAttribute(final XmlSchemaElement element, final XmlSchemaAnyAttribute anyAttr) {  }

  /**
   * Retrieves the <code>StackEntry</code> representing the parent element.
   * Traverses through as many substitution groups as necessary to find it.
   *
   * @return The <code>StackEntry</code> representing the parent element.
   * @exception IllegalStateException if there is no parent element.
   */
  private StackEntry getParentElement() {
    ListIterator<StackEntry> iterator = stack.listIterator( stack.size() );
    while ( iterator.hasPrevious() ) {
      StackEntry entry = iterator.previous();
      if (!entry.isSubstitutionGroup) {
        return entry;
      }
    }

    throw new IllegalStateException("No parent element available in stack.");
  }

  /**
   * If we are processing a substitution group, returns the
   * <code>StackEntry</code> representing that substitution
   * group.  Otherwise, returns <code>null</code>.
   *
   * @return A <code>StackEntry</code> for the represented
   *         substitution group, or <code>null</code> if none.
   */
  private StackEntry getSubstitutionGroup() {
    final ListIterator<StackEntry> iterator = stack.listIterator( stack.size() );
    if (iterator.hasPrevious()) {
      StackEntry prev = iterator.previous();
      if (prev.isSubstitutionGroup) {
        return prev;
      }
    }
    return null;
  }

  private void addSchemaToParent(final Schema schema) {
    final StackEntry parent = getParentElement();
    List<Schema> siblings = fieldsByElement.get(parent.elementQName);
    if (siblings == null) {
      siblings = new ArrayList<Schema>();
      fieldsByElement.put(parent.elementQName, siblings);
    }
    siblings.add(schema);
  }

  private StackEntry pop(QName entryQName, boolean isSubstGroup) {
    if ( stack.isEmpty() ) {
      throw new IllegalStateException("Attempted to pop " + getStackEntryInfo(entryQName, isSubstGroup) + " off of an empty stack.");
    }

    final StackEntry entry = stack.get(stack.size() - 1);
    stack.remove(stack.size() - 1);

    if (!entry.elementQName.equals(entryQName) || (entry.isSubstitutionGroup != isSubstGroup)) {
      throw new IllegalStateException("Attempted to pop " + getStackEntryInfo(entryQName, isSubstGroup) + " but found " + getStackEntryInfo(entry.elementQName, entry.isSubstitutionGroup));
    }

    return entry;
  }

  private static String getStackEntryInfo(QName entryQName, boolean isSubstGroup) {
    return "\"" + entryQName + "\" (Substitution Group? " + isSubstGroup + ")";
  }

  private static String getDocumentationFor(XmlSchemaAnnotation annotation) {
    if ((annotation != null)
        && (annotation.getItems() != null)
        && !annotation.getItems().isEmpty()) {

      StringBuilder docs = new StringBuilder();
      for (XmlSchemaAnnotationItem item : annotation.getItems()) {
        if (item instanceof XmlSchemaDocumentation) {
          final NodeList docNodes = ((XmlSchemaDocumentation) item).getMarkup();
          for (int nodeIndex = 0; nodeIndex < docNodes.getLength(); ++nodeIndex) {
            docs.append( docNodes.item(nodeIndex).getTextContent().replaceAll("\\s+", " ") );
          }
          break;
        }
      }
      return docs.toString();
    }

    return null;
  }

  private Schema root;
  private final ArrayList<StackEntry> stack;
  private final Map<QName, Schema> schemasByElement;
  private final Map<QName, List<Schema>> substitutionGroups;
  private final Map<QName, List<Schema>> fieldsByElement;
  private final Map<QName, List<Schema.Field>> attributesByElement; 
}
