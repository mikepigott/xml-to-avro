/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.xml;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ws.commons.schema.constants.Constants;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.w3c.dom.NodeList;

/**
 * Generates an Avro schema based on the walked XML Schema.
 */
final class AvroSchemaGenerator implements XmlSchemaVisitor {

  private Schema root;

  private final List<URL> schemaUrls;
  private final List<File> schemaFiles;
  private final String baseUri;

  private final ArrayList<StackEntry> stack;
  private final Map<QName, Schema> schemasByElement;
  private final Map<QName, List<Schema>> substitutionGroups;
  private final Map<QName, List<Schema>> fieldsByElement;
  private final Map<QName, List<AttributeEntry>> attributesByElement; 

  private static class StackEntry {
    final QName elementQName;
    final boolean isSubstitutionGroup;

    public StackEntry(
        final QName elementQName,
        final boolean isSubstitutionGroup) {

      this.elementQName = elementQName;
      this.isSubstitutionGroup = isSubstitutionGroup;
    }
  }

  private static class AttributeEntry {
    private final Schema.Field schemaField;
    private final boolean isNonOptionalIdField;

    AttributeEntry(Schema.Field field, boolean isNonOptionalIdField) {
      this.schemaField = field;
      this.isNonOptionalIdField = isNonOptionalIdField;
    }

    Schema.Field getField() {
      return schemaField;
    }

    boolean isNonOptionalIdField() {
      return isNonOptionalIdField;
    }
  }

  AvroSchemaGenerator(
      String baseUri,
      List<URL> schemaUrls,
      List<File> schemaFiles) {

    this.baseUri = baseUri;
    this.schemaUrls = schemaUrls;
    this.schemaFiles = schemaFiles;

    root = null;
    stack = new ArrayList<StackEntry>();
    schemasByElement = new HashMap<QName, Schema>();
    substitutionGroups = new HashMap<QName, List<Schema>>();
    fieldsByElement = new HashMap<QName, List<Schema>>();
    attributesByElement = new HashMap<QName, List<AttributeEntry>>();
  }

  /**
   * Clears the internal state of the {@link AvroSchemaGenerator}
   * so a new Avro {@link Schema} can be generated from a new
   * {@link org.apache.ws.commons.schema.XmlSchema}.
   */
  void clear() {
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
  Schema getSchema() {
    return root;
  }

  /**
   * If this element was not added previously, registers a new schema for it.
   * If this element was added previously, and it is not part of a substitution
   * group, add it to its parent.
   *
   * @see XmlSchemaVisitor#onEnterElement(XmlSchemaElement, XmlSchemaTypeInfo, boolean)
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
          throw new IllegalStateException(
              "Element \""
              + element.getQName()
              + "\" was previously visited, but has no schema.");
        }

        addSchemaToParent(schema);
      }

      return;
    }

    /* If this element is abstract, it is not
     * a member of the substitution group.
     */
    if ((substGrp != null) && element.isAbstract()) {
      return;
    }

    // If documentation is available, makes it the record's documentation.
    final String documentation =
        getDocumentationFor( element.getAnnotation() );

    // Create the record.
    Schema record = null;
    String avroNamespace = null;

    try {
      avroNamespace = Utils.getAvroNamespaceFor( elemQName.getNamespaceURI() );
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Element \""
          + elemQName
          + "\" has an invalid namespace of \""
          + elemQName.getNamespaceURI() + "\"", e);
    }

    record =
        Schema.createRecord(
            elemQName.getLocalPart(),
            documentation,
            avroNamespace,
            false);

    schemasByElement.put(elemQName, record);
  }

  /**
   * If this element was not visited previously, retrieves all of the fields
   * associated with the record corresponding to the element, and adds them.
   *
   * @see XmlSchemaVisitor#onExitElement(XmlSchemaElement, XmlSchemaTypeInfo, boolean)
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

    Schema record = schemasByElement.get(entry.elementQName);
    if (record == null) {
      throw new IllegalStateException(
          "No schema found for element \"" + entry.elementQName + "\".");

    } else if (record.getType().equals(Schema.Type.MAP)) {
      record = record.getValueType();
    }

    final List<AttributeEntry> fields =
        attributesByElement.get(entry.elementQName);

    ArrayList<Schema.Field> schemaFields = null; 

    if (fields == null) {
      schemaFields = new ArrayList<Schema.Field>(1);
    } else {
      schemaFields = new ArrayList<Schema.Field>( fields.size() );
      for (AttributeEntry attrEntry : fields) {
        schemaFields.add( attrEntry.getField() );
      }
    }

    final List<Schema> children = fieldsByElement.get(entry.elementQName);

    /* If there are multiple maps in the children, merge
     * them together under one MAP of UNION of children.
     */
    if ((children != null) && !children.isEmpty()) {
      int mapIndex = -1;
      ArrayList<Schema> mapUnion = null;
      ArrayList<Integer> indicesToRemove = null;
      for (int i = 0; i < children.size(); ++i) {
        Schema currSchema = children.get(i);
        if (currSchema.getType().equals(Schema.Type.MAP)) {
          if (mapIndex < 0) {
            // This is the first map.
            mapIndex = i;
          } else if (mapUnion == null) {
            // This is the second map.
            mapUnion = new ArrayList<Schema>();
            final Schema mapSchema = children.get(mapIndex);
            mapUnion.add( mapSchema.getValueType() );
            mapUnion.add( currSchema.getValueType() );

            indicesToRemove = new ArrayList<Integer>();
            indicesToRemove.add(i);

          } else {
            // These are maps 3+.
            mapUnion.add( currSchema.getValueType() );
            indicesToRemove.add(i);
          }
        }
      }

      if (mapUnion != null) {
        // 1. Create a union of all MAP children.
        children.set(
            mapIndex,
            Schema.createMap( Schema.createUnion(mapUnion) ));

        // 2. Remove all other indices with MAPs in them.
        ListIterator<Integer> iter =
            indicesToRemove.listIterator(indicesToRemove.size());

        while ( iter.hasPrevious() ) {
          children.remove( iter.previous().intValue() );
        }
      }

      // Now, remove duplicate children.
      final HashSet<String> duplicates = new HashSet<String>();

      if (indicesToRemove != null) {
        indicesToRemove.clear();
      }

      for (int childIndex = 0; childIndex < children.size(); ++childIndex) {
        Schema child = children.get(childIndex);
        if ( duplicates.contains( child.getFullName() ) ) {
          if (indicesToRemove == null) {
            indicesToRemove = new ArrayList<Integer>();
          }
          indicesToRemove.add(childIndex);
        } else {
          duplicates.add( child.getFullName() );
        }
      }

      if ((indicesToRemove != null) && !indicesToRemove.isEmpty()) {
        ListIterator<Integer> iter =
            indicesToRemove.listIterator(indicesToRemove.size());

        while ( iter.hasPrevious() ) {
          children.remove( iter.previous().intValue() );
        }
      }
    }

    if ((children != null)
          && !children.isEmpty()
          && (typeInfo != null)
          && (typeInfo.getUserRecognizedType() != null)) {
      throw new IllegalStateException(
          "Element \"" + entry.elementQName + "\" has both a type ("
          + typeInfo.getUserRecognizedType() + ") and " + children.size()
          + " child elements.");

    } else if ((children != null) && !children.isEmpty()) {
      boolean isMixedType = false;
      if (typeInfo != null) {
        isMixedType = typeInfo.isMixed();
      }

      if (isMixedType) {
        boolean foundString = false;
        for (Schema child : children) {
          if ( child.getType().equals(Schema.Type.STRING) ) {
            foundString = true;
            break;
          }
        }
        if (!foundString) {
          children.add( Schema.create(Schema.Type.STRING) );
        }
      }

      final Schema schema = Schema.createArray( Schema.createUnion(children) );
      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              schema,
              "Children of " + entry.elementQName,
              null);
      schemaFields.add(field);

    } else if ((typeInfo != null)
                && (typeInfo.getType().equals(XmlSchemaTypeInfo.Type.LIST)
                    || typeInfo.getType().equals(XmlSchemaTypeInfo.Type.UNION)
                    || typeInfo.isMixed()
                    || (typeInfo.getUserRecognizedType() != null))) {

      final Schema childSchema =
          Utils.getAvroSchemaFor(
              typeInfo,
              element.getQName(),
              element.isNillable());

      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              childSchema,
              "Simple type " + typeInfo.getUserRecognizedType(),
              null);

      schemaFields.add(field);

    } else if (((children == null) || children.isEmpty())
               && ((typeInfo == null)
                   || (typeInfo.getUserRecognizedType() == null))) {

      // This element has no children.  Set a null placeholder.
      final Schema.Field field =
          new Schema.Field(
              entry.elementQName.getLocalPart(),
              Schema.create(Type.NULL),
              "This element contains no attributes and no children.",
              null);
      schemaFields.add(field);
    }

    record.setFields(schemaFields);
  }

  /**
   * Processes the attribute of the provided element,
   * adding it as a field to the corresponding record.
   *
   * @see XmlSchemaVisitor#onVisitAttribute(XmlSchemaElement, XmlSchemaAttribute, XmlSchemaTypeInfo)
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

    final QName elemQName = element.getQName();
    final QName attrQName = attribute.getQName();

    final String documentation =
        getDocumentationFor( attribute.getAnnotation() );

    boolean isOptional = false;

    // Optional types are unions of the real type and null.
    if (attribute.getUse().equals(XmlSchemaUse.OPTIONAL)
        && (attribute.getDefaultValue() == null)
        && (attribute.getFixedValue() == null)) {
      isOptional = true;
    }

    boolean isIdField = false;
    if ((attributeType.getUserRecognizedType() != null)
        && attributeType.getUserRecognizedType().equals(Constants.XSD_ID)) {
      isIdField = true;
    }

    Schema attrSchema =
        Utils.getAvroSchemaFor(
            attributeType,
            attribute.getQName(),
            isOptional);

    final Schema.Field attr =
        new Schema.Field(
            attrQName.getLocalPart(),
            attrSchema,
            documentation,
            null);

    List<AttributeEntry> attrs = attributesByElement.get(elemQName);
    if (attrs == null) {
      attrs = new ArrayList<AttributeEntry>();
      attributesByElement.put(elemQName, attrs);
    }
    attrs.add( new AttributeEntry(attr, isIdField && !isOptional) );
  }

  @Override
  public void onEndAttributes(
      XmlSchemaElement element,
      XmlSchemaTypeInfo elemTypeInfo) {

    final QName elemQName = element.getQName();
    final StackEntry substGrp = getSubstitutionGroup();

    if ((substGrp != null) && element.isAbstract()) {
      return;
    }

    final List<AttributeEntry> fields =
        attributesByElement.get(elemQName);

    Schema record = schemasByElement.get(elemQName);
    if (record == null) {
      throw new IllegalStateException(
          "No schema found for element \"" + elemQName + "\".");
    }

    /* If this RECORD contains exactly one non-optional ID attribute, it is
     * better served as a MAP.  However, the root element of a document cannot
     * be a map; it would have no siblings.
     */
    if (!stack.isEmpty()
        && ((stack.size() > 1)
            || !stack.get(0).isSubstitutionGroup)
        && isMap(fields)) {
      record = Schema.createMap(record);
      schemasByElement.put(elemQName, record);
    }

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

    } else if ( stack.isEmpty() ) {
      // This is the root element!
      root = record;
      addXmlSchemasListToRoot( element.getQName() );

    } else {
      /* This is not part of a substitution group,
       * and not the root.  Add it to its parent.
       */
      addSchemaToParent(record);
    }

    stack.add( new StackEntry(elemQName, false) );
  }

  /**
   * Adds a new stack entry for this substitution group.
   *
   * @see XmlSchemaVisitor#onEnterSubstitutionGroup(XmlSchemaElement)
   */
  @Override
  public void onEnterSubstitutionGroup(XmlSchemaElement base) {
    stack.add( new StackEntry(base.getQName(), true) );
  }

  /**
   * Retrieves all of the members of this substitution
   * group and adds them to the parent as children.
   *
   * @see XmlSchemaVisitor#onExitSubstitutionGroup(XmlSchemaElement)
   */
  @Override
  public void onExitSubstitutionGroup(XmlSchemaElement base) {
    final StackEntry entry = pop(base.getQName(), true);
    final List<Schema> substitutes =
        substitutionGroups.get(entry.elementQName);

    if ((substitutes == null) || substitutes.isEmpty()) {
      /* This happens when an abstract element can only
       * be substituted by other abstract elements.
       */
      return;
    }

    if ( stack.isEmpty() ) {
      // The root node in the stack is part of a substitution group.
      root = Schema.createUnion(substitutes);
      addXmlSchemasListToRoot( base.getQName() );

    } else {
      // The substitution group is part of a higher group.
      StackEntry parent = getParentElement();
      List<Schema> siblings = fieldsByElement.get(parent.elementQName);
      if (siblings == null) {
        siblings = new ArrayList<Schema>( substitutes.size() );
      }
      siblings.addAll(substitutes);
    }
  }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onEnterAllGroup(XmlSchemaAll)
   */
  @Override
  public void onEnterAllGroup(XmlSchemaAll all) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onExitAllGroup(XmlSchemaAll)
   */
  @Override
  public void onExitAllGroup(XmlSchemaAll all) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onEnterChoiceGroup(XmlSchemaChoice)
   */
  @Override
  public void onEnterChoiceGroup(XmlSchemaChoice choice) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onExitChoiceGroup(XmlSchemaChoice)
   */
  @Override
  public void onExitChoiceGroup(XmlSchemaChoice choice) { }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onEnterSequenceGroup(XmlSchemaSequence)
   */
  @Override
  public void onEnterSequenceGroup(final XmlSchemaSequence seq) {  }

  /**
   * Avro schemas do not handle different group
   * types differently.  This is a no-op.
   *
   * @see XmlSchemaVisitor#onExitSequenceGroup(XmlSchemaSequence)
   */
  @Override
  public void onExitSequenceGroup(final XmlSchemaSequence seq) { }

  /**
   * Avro schemas do not have support for an
   * "anything" type. This method is a no-op.
   *
   * @see XmlSchemaVisitor#onVisitAny(XmlSchemaAny)
   */
  @Override
  public void onVisitAny(XmlSchemaAny any) { }

  /**
   * Avro schemas do not have support for an
   * "anything" type. This method is a no-op.
   *
   * @see XmlSchemaVisitor#onVisitAnyAttribute(XmlSchemaElement, XmlSchemaAnyAttribute)
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
    final ListIterator<StackEntry> iterator = stack.listIterator(stack.size());
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
      throw new IllegalStateException(
          "Attempted to pop "
          + getStackEntryInfo(entryQName, isSubstGroup)
          + " off of an empty stack.");
    }

    final StackEntry entry = stack.remove(stack.size() - 1);

    if (!entry.elementQName.equals(entryQName)
        || (entry.isSubstitutionGroup != isSubstGroup)) {
      throw new IllegalStateException(
          "Attempted to pop "
          + getStackEntryInfo(entryQName, isSubstGroup)
          + " but found "
          + getStackEntryInfo(entry.elementQName, entry.isSubstitutionGroup));
    }

    return entry;
  }

  private void addXmlSchemasListToRoot(QName rootTagQName) {
    if (((schemaUrls == null) || schemaUrls.isEmpty())
        && ((schemaFiles == null) || schemaFiles.isEmpty())
        && ((baseUri == null) || !baseUri.isEmpty())) {
      return;
    }

    final ObjectNode schemasNode = JsonNodeFactory.instance.objectNode();

    if ((schemaUrls != null) && !schemaUrls.isEmpty()) {
      final ArrayNode urlArrayNode = JsonNodeFactory.instance.arrayNode();
      for (URL schemaUrl : schemaUrls) {
        urlArrayNode.add( schemaUrl.toString() );
      }
      schemasNode.put("urls", urlArrayNode);
    }

    if ((schemaFiles != null) && !schemaFiles.isEmpty()) {
      final ArrayNode fileArrayNode = JsonNodeFactory.instance.arrayNode();
      for (File schemaFile : schemaFiles) {
        fileArrayNode.add( schemaFile.getAbsolutePath() );
      }
      schemasNode.put("files", fileArrayNode);
    }

    if ((baseUri != null) && !baseUri.isEmpty()) {
      schemasNode.put("baseUri", baseUri);
    }

    final ObjectNode rootTagNode = JsonNodeFactory.instance.objectNode();
    rootTagNode.put("namespace", rootTagQName.getNamespaceURI());
    rootTagNode.put("localPart", rootTagQName.getLocalPart());

    schemasNode.put("rootTag", rootTagNode);

    if ( root.getType().equals(Schema.Type.RECORD) ) {
      root.addProp("xmlSchemas", schemasNode);

    } else if ( root.getType().equals(Schema.Type.UNION) ) {
      if ((root.getTypes() == null) || root.getTypes().isEmpty()) {
        throw new IllegalStateException(
            "Root is a substitution group with no children!");
      }

      final Schema firstElem = root.getTypes().get(0);
      if ( !firstElem.getType().equals(Schema.Type.RECORD) ) {
        throw new IllegalStateException(
            "Root is a substitution group with a first element of type "
            + firstElem.getType());
      }

      firstElem.addProp("xmlSchemas", schemasNode);

    } else {
      throw new IllegalStateException(
          "Document root is neither a RECORD nor a UNION.");
    }
  }

  private static boolean isMap(final List<AttributeEntry> attributes) {

    int nonOptionalIdFieldCount = 0;

    if (attributes != null) {
      for (AttributeEntry attribute : attributes) {
        if ( attribute.isNonOptionalIdField() ) {
          ++nonOptionalIdFieldCount;
        }
      }
    }

    return (nonOptionalIdFieldCount == 1);
  }

  private static String getStackEntryInfo(
      QName entryQName,
      boolean isSubstGroup) {
    return "\"" + entryQName + "\" (Substitution Group? " + isSubstGroup + ")";
  }

  private static String getDocumentationFor(XmlSchemaAnnotation annotation) {
    if ((annotation != null)
        && (annotation.getItems() != null)
        && !annotation.getItems().isEmpty()) {

      StringBuilder docs = new StringBuilder();
      for (XmlSchemaAnnotationItem item : annotation.getItems()) {
        if (item instanceof XmlSchemaDocumentation) {
          final NodeList docNodes =
              ((XmlSchemaDocumentation) item).getMarkup();

          for (int nodeIdx = 0; nodeIdx < docNodes.getLength(); ++nodeIdx) {
            docs.append(
                docNodes
                  .item(nodeIdx)
                  .getTextContent()
                  .replaceAll("\\s+", " "));
          }
          break;
        }
      }
      return docs.toString();
    }

    return null;
  }
}
