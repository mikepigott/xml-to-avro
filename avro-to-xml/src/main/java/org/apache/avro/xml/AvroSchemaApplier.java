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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaUse;

/**
 * Applies an Avro schema to a tree described by
 * {@link XmlSchemaDocumentNode}s and {@link XmlSchemaDocumentPathNode}s.
 *
 * <p>
 * Schema evolution is handled with the following conversions:
 * <ul>
 *   <li>STRING, BOOLEAN, ENUM, DOUBLE, FLOAT, LONG, INT -> STRING</li>
 *   <li>DOUBLE, FLOAT, LONG, INT -> DOUBLE</li>
 *   <li>FLOAT, LONG, INT -> FLOAT</li>
 *   <li>LONG, INT -> LONG</li>
 *   <li>INT -> INT</li>
 *   <li>BOOLEAN -> BOOLEAN</li>
 *   <li>BYTES -> BYTES</li>
 *   <li>ENUM -> ENUM when destination ENUM is a superset of the source.</li>
 *   <li>RECORD -> RECORD when all the fields can be converted as well.</li>
 * </ul>
 * </p>
 *
 * <p>
 * Also joins sibling map elements under the same map,
 * and tracks the content nodes of a mixed element.
 * </p>
 */
final class AvroSchemaApplier {

  private List<Schema> unionOfValidElementsStack;
  private List<AvroRecordInfo> avroRecordStack;

  private final Schema avroSchema;
  private final Map<Schema.Type, Set<Schema.Type>> conversionCache;
  private final boolean xmlIsWritten;

  /**
   * {@link XmlSchemaPathNode} contain their destination
   * {@link XmlSchemaDocumentNode}, but not their originating
   * one.  Since we do not "leave" a {@link XmlSchemaDocumentNode}
   * until we traverse to its parent, we need to track the parent
   * node in addition to the current one.
   */
  private static class StackEntry {
    StackEntry(XmlSchemaDocumentNode<AvroRecordInfo> docNode) {
      this.docNode = docNode;
      this.parentNode = docNode.getParent();
    }

    final XmlSchemaDocumentNode<AvroRecordInfo> docNode;
    final XmlSchemaDocumentNode<AvroRecordInfo> parentNode;
    int occurrence;
  }

  /**
   * Creates a new <code>AvroSchemaApplier</code>
   * with the provided root node.
   */
  AvroSchemaApplier(Schema avroSchema, boolean xmlIsWritten) {
    this.avroSchema = avroSchema;
    this.xmlIsWritten = xmlIsWritten;

    conversionCache = new HashMap<Schema.Type, Set<Schema.Type>>();
    unionOfValidElementsStack = new ArrayList<Schema>();
    avroRecordStack = new ArrayList<AvroRecordInfo>();

    if ( avroSchema.getType().equals(Schema.Type.ARRAY) ) {
      // ARRAY of UNION of RECORDs/MAPs is not valid when writing XML.
      if (xmlIsWritten) {
        throw new IllegalArgumentException(
            "The Avro Schema cannot be an ARRAY of UNION of MAPs/RECORDs when "
            + "writing XML; it must conform to the corresponding XML schema.");
      }

      /* The user is only looking to retrieve specific elements from the XML
       * document.  Likewise, the next valid elements are only the ones in
       * that list.
       *
       * (The expected format is Array<Union<Type>>)
       */
      if ( !avroSchema.getElementType().getType().equals(Schema.Type.UNION) ) {
        throw new IllegalArgumentException(
            "If retrieving only a subset of elements in the document, the Avro"
            + " Schema must be an ARRAY of UNION of those types, not an ARRAY"
            + " of "
            + avroSchema.getElementType().getType());
      }

      // Confirm all of the elements in the UNION are either RECORDs or MAPs.
      verifyIsUnionOfMapsAndRecords(avroSchema.getElementType(), true);

      unionOfValidElementsStack.add(avroSchema.getElementType());

    } else if ( avroSchema.getType().equals(Schema.Type.UNION) ) {
      /* It is possible for the root element to actually be the root of a
       * substitution group.  If this happens, the root element could be
       * one of many different record types.
       *
       * This can only be valid if the schema is a union of records.
       */
      verifyIsUnionOfMapsAndRecords(avroSchema, true);

      unionOfValidElementsStack.add(avroSchema);

    } else if ( avroSchema.getType().equals(Schema.Type.RECORD)
        || avroSchema.getType().equals(Schema.Type.MAP) ) {
      // This is a definition of the root element.
      List<Schema> union = new ArrayList<Schema>(1);
      union.add(avroSchema);
      unionOfValidElementsStack.add( Schema.createUnion(union) );

    } else {
      throw new IllegalArgumentException(
          "The Avro Schema must be one of the following types: RECORD, MAP,"
          + " UNION of RECORDs/MAPs, or ARRAY of UNION of RECORDs/MAPs.");
      
    }
  }

  void apply(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> pathStart) {

    // Add schema information to the document tree.
    apply(pathStart.getDocumentNode());

    // Count maps.
    findMaps(pathStart);

    // Update child count for mixed elements.
    applyContent(pathStart);
  }

  private void apply(XmlSchemaDocumentNode<AvroRecordInfo> docNode) {
    switch (docNode.getStateMachineNode().getNodeType()) {
    case ELEMENT:
      processElement(docNode);
      break;
    case ALL:
    case CHOICE:
    case SEQUENCE:
    case SUBSTITUTION_GROUP:
      processGroup(docNode);
      break;
    case ANY:
      // Ignored
      break;
    default:
      throw new IllegalArgumentException(
          "Document node has an unrecognized type of "
          + docNode.getStateMachineNode().getNodeType()
          + '.');
    }
  }

  private void processElement(XmlSchemaDocumentNode<AvroRecordInfo> doc) {
    if (!doc
           .getStateMachineNode()
           .getNodeType()
           .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalStateException(
          "Attempted to process an element when the node type is "
          + doc.getStateMachineNode().getNodeType());
    }

    final XmlSchemaElement element = doc.getStateMachineNode().getElement();

    final List<Schema> validNextElements =
        unionOfValidElementsStack
          .get(unionOfValidElementsStack.size() - 1)
          .getTypes();

    Schema elemSchema = null;
    int schemaIndex = 0;
    int mapSchemaIndex = -1;

    if (validNextElements != null) {
      for (; schemaIndex < validNextElements.size(); ++schemaIndex) {
        Schema possibleSchema = validNextElements.get(schemaIndex);
        Schema valueType = possibleSchema;

        if ( possibleSchema.getType().equals(Schema.Type.MAP) ) {
          valueType = possibleSchema.getValueType();

          if ( valueType.getType().equals(Schema.Type.UNION) ) {
            /* This XML document has multiple sibling tags representable as
             * MAPs.  We need to cycle through them and find the best fit.
             */
            for (mapSchemaIndex = 0;
                mapSchemaIndex < valueType.getTypes().size();
                ++mapSchemaIndex) {
              final Schema unionType = valueType.getTypes().get(mapSchemaIndex);
              if ( !unionType.getType().equals(Schema.Type.RECORD) ) {
                throw new IllegalStateException(
                    "MAPs in Avro Schemas for XML documents must have a value"
                    + " type of either RECORD or UNION of RECORD, not UNION"
                    + " with "
                    + unionType.getType());
              }
              if (typeMatchesElement(unionType, element)) {
                elemSchema = possibleSchema;
                break;
              }
            }

            /* If we walked through all of the map elements and did
             * not find a matching UNION, reset the mapSchemaIndex
             * and check the next candidate.
             */
            if (elemSchema == null) {
              mapSchemaIndex = -1;
              continue;
            } else {
              // We found the element!  Stop looking.
              break;
            }
          }
        }

        if ( !valueType.getType().equals(Schema.Type.RECORD) ) {
          throw new IllegalStateException(
              "RECORD, MAP of RECORD, and MAP of UNION of RECORD are allowed. "
              + valueType.getType()
              + " cannot exist in any level of that hierarchy.");
        }

        /* If we reach here, we have not found the schema, and valueType is of
         * type RECORD (either the original RECORD or the child of a MAP) and
         * needs to be checked.
         */
        if (typeMatchesElement(valueType, element)) {
          elemSchema = possibleSchema;
          break;
        }
      }
    }

    if (xmlIsWritten && (elemSchema == null)) {
      throw new IllegalStateException(
          "Element \""
          + element.getQName()
          + "\" does not have a corresponding Avro schema.  One is needed when"
          + " writing XML.");
    }

    final XmlSchemaTypeInfo typeInfo =
        doc.getStateMachineNode().getElementType();

    Schema unionOfChildrenTypes = null;

    if (elemSchema != null) {
      final List<XmlSchemaStateMachineNode.Attribute> attributes =
          doc.getStateMachineNode().getAttributes();

      // Match the element's attributes against the element's schema.
      for (XmlSchemaStateMachineNode.Attribute attribute : attributes) {
        processAttribute(
            element.getQName(),
            elemSchema,
            attribute.getAttribute(),
            attribute.getType(),
            mapSchemaIndex);
      }

      /* Child elements are in a field under the same name as the element.
       *
       * In the Avro schema, they may be NULL (no children), a
       * primitive type, or an ARRAY of UNION of MAPs and RECORDs.
       */
      Schema valueType = elemSchema;
      if (elemSchema.getType().equals(Schema.Type.MAP)) {
        valueType = elemSchema.getValueType();
        if (mapSchemaIndex >= 0) {
          valueType = valueType.getTypes().get(mapSchemaIndex);
        }
      }

      Schema.Field childrenField = valueType.getField( element.getName() );

      /* If the element has no children, a NULL placeholder is used instead.
       * Likewise, if the children field is null, it means the children have
       * been removed in order to be filtered out. 
       */
      if (xmlIsWritten && (childrenField == null)) {
        throw new IllegalStateException(
            "The children of "
            + element.getQName()
            + " in Avro Schema {"
            + elemSchema.getNamespace()
            + "}"
            + elemSchema.getName()
            + " must exist.  If there are no children, an Avro NULL"
            + " placeholder is required.");
      }

      if (childrenField != null) {
        final Schema childrenSchema = childrenField.schema();
        switch (childrenSchema.getType()) {
        case ARRAY:
          {
            if (typeInfo.getType().equals(XmlSchemaTypeInfo.Type.LIST)) {
              break;
            }

            // All group types are ARRAY of UNION of MAP/RECORD.
            if ( !childrenSchema
                    .getElementType()
                    .getType()
                    .equals(Schema.Type.UNION) ) {
              throw new IllegalStateException(
                  "If the children of "
                  + element.getQName()
                  + " in Avro Schema {"
                  + elemSchema.getNamespace()
                  + "}"
                  + elemSchema.getName()
                  + " are in a group, the corresponding Avro Schema MUST BE an"
                  + " ARRAY of UNION of MAPs/RECORDs, not "
                  + childrenSchema.getElementType().getType());
            }

            verifyIsUnionOfMapsAndRecords(
                childrenSchema.getElementType(),
                typeInfo.isMixed());

            unionOfChildrenTypes = childrenSchema.getElementType();
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
        case RECORD:
          {
            if (!confirmEquivalent(
                    typeInfo,
                    element.getQName(),
                    childrenSchema) ) {
              throw new IllegalStateException(
                  "Cannot convert between "
                  + typeInfo
                  + " and "
                  + childrenSchema
                  + " for simple content of "
                  + element.getQName()
                  + " in Avro Schema {"
                  + elemSchema.getNamespace()
                  + "}"
                  + elemSchema.getName());
            }
          }
          break;
        case NULL:
          // There are no children, so no further types are valid.
          break;
        case UNION:
          if (typeInfo.getType().equals(XmlSchemaTypeInfo.Type.UNION)) {
            break;
          } else if (element.isNillable()
                      && (childrenSchema.getTypes().size() == 2)) {
            break;
          }
        default:
          throw new IllegalStateException(
              "Children of element "
              + element.getQName()
              + " in Avro Schema {"
              + elemSchema.getNamespace()
              + "}"
              + elemSchema.getName()
              + " must be either an ARRAY of UNION of MAP/RECORD or a"
              + " primitive type, not "
              + childrenSchema.getType());
        }
      }

      AvroRecordInfo recordInfo = null;
      if (avroRecordStack.isEmpty() && (doc.getParent() == null)) {
        recordInfo = new AvroRecordInfo(elemSchema);
        avroRecordStack.add(recordInfo);
      } else {
        recordInfo =
            new AvroRecordInfo(elemSchema, schemaIndex, mapSchemaIndex);

        /* Maps will be counted separately, as their
         * children are not part of this array.
         *
         * The stack will be empty if the root element
         * is part of a substitution group.
         */
        if (!elemSchema.getType().equals(Schema.Type.MAP)
            && !avroRecordStack.isEmpty()) {

          for (int docIter = 0; docIter < doc.getIteration(); ++docIter) {
            avroRecordStack
              .get(avroRecordStack.size() - 1)
              .incrementChildCount();
          }
        }
        avroRecordStack.add(recordInfo);
      }
      doc.setUserDefinedContent(recordInfo);
    }

    /* If the root schema is an ARRAY of UNION, then the next valid
     * element will be one of its entries.  Otherwise, there are no
     * next valid entries.
     *
     * We want to push that on the stack for when we exit children
     * of the current element.
     */
    if ((unionOfChildrenTypes == null)
          && avroSchema.getType().equals(Schema.Type.ARRAY) ) {
      unionOfChildrenTypes = avroSchema.getElementType();
    }

    // Process the children, if any.
    if (unionOfChildrenTypes != null) {
      unionOfValidElementsStack.add(unionOfChildrenTypes);
      processChildren(doc);
      unionOfValidElementsStack.remove(unionOfValidElementsStack.size() - 1);
    }

    if (elemSchema != null) {
      avroRecordStack.remove(avroRecordStack.size() - 1);
    }
  }

  private void processAttribute(
      QName elementName,
      Schema elementSchema,
      XmlSchemaAttribute attribute,
      XmlSchemaTypeInfo attributeType,
      int mapUnionIndex) {

    Schema valueType = elementSchema;
    if ( valueType.getType().equals(Schema.Type.MAP) ) {
      valueType = valueType.getValueType();
      if (mapUnionIndex >= 0) {
        valueType = valueType.getTypes().get(mapUnionIndex);
      }
    }

    final Schema.Field attrField = valueType.getField( attribute.getName() );

    if (xmlIsWritten
        && (attrField == null)
        && !attribute.getUse().equals(XmlSchemaUse.OPTIONAL)
        && !attribute.getUse().equals(XmlSchemaUse.PROHIBITED)) {
      throw new IllegalStateException(
          "Element "
          + elementName
          + " has a "
          + attribute.getUse()
          + " attribute named "
          + attribute.getQName()
          + " - when writing to XML, a field in the Avro record must exist.");
    }

    if (attrField != null) {
      Schema attrType = attrField.schema();

      if ( attribute.getUse().equals(XmlSchemaUse.OPTIONAL) 
          && attrType.getType().equals(Schema.Type.UNION) ) {

        /* The XML Schema Attribute may have already been a union, so we
         * need to walk all of the subtypes and pull out the non-NULL ones.
         */
        final ArrayList<Schema> subset =
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

      if (!confirmEquivalent(
          attributeType,
          attribute.getQName(),
          attrType)) {
        throw new IllegalStateException(
            "Cannot convert element "
            + elementName
            + " attribute "
            + attribute.getQName()
            + " types between "
            + attributeType.getBaseType()
            + " and "
            + attrField.schema());
      }
    }
  }

  private void processChildren(XmlSchemaDocumentNode<AvroRecordInfo> doc) {
    for (int iteration = 1; iteration <= doc.getIteration(); ++iteration) {
      final SortedMap<Integer, XmlSchemaDocumentNode<AvroRecordInfo>>
        children = doc.getChildren(iteration);

      if (children != null) {
        for (Map.Entry<Integer, XmlSchemaDocumentNode<AvroRecordInfo>> child :
              children.entrySet()) {
          apply(child.getValue());
        }
      }
    }
  }

  private void processGroup(XmlSchemaDocumentNode<AvroRecordInfo> doc) {
    /* The union of valid types is already on the stack from
     * the owning element.  We just need to walk the children.
     */
    switch( doc.getStateMachineNode().getNodeType() ) {
    case SUBSTITUTION_GROUP:
    case ALL:
    case CHOICE:
    case SEQUENCE:
      processChildren(doc);
      break;
    default:
      throw new IllegalStateException(
          "Attempted to process a group, but the document node is of type "
          + doc.getStateMachineNode().getNodeType());
    }
  }

  // Confirms the root-level Schema is a UNION of MAPs, RECORDs, or both.
  private static void verifyIsUnionOfMapsAndRecords(
      Schema schema,
      boolean isMixed) {

    for (Schema unionType : schema.getTypes()) {
      if (!unionType.getType().equals(Schema.Type.RECORD)
          && !unionType.getType().equals(Schema.Type.MAP)
          && !(isMixed && unionType.getType().equals(Schema.Type.STRING))) {

        throw new IllegalArgumentException(
            "The Avro Schema may either be a UNION or an ARRAY of UNION, but"
            + " only if all of the elements in the UNION are of either type"
            + " RECORD or MAP, not "
            + unionType.getType());

      } else if (unionType.getType().equals(Schema.Type.MAP)) {
        if ( unionType.getValueType().getType().equals(Schema.Type.UNION) ) {
          for (Schema mapUnionType : unionType.getValueType().getTypes()) {
            if (!mapUnionType.getType().equals(Schema.Type.RECORD)) {
              throw new IllegalArgumentException(
                  "If using a UNION of MAP of UNION, all of the UNION types"
                  + " must be RECORD, not "
                  + mapUnionType.getType());
            }
          }
        } else if (
            !unionType
               .getValueType()
               .getType()
               .equals(Schema.Type.RECORD)) {

          throw new IllegalArgumentException(
              "If the Avro Schema is a UNION of MAPs or an ARRAY of UNION of"
              + " MAPs, all MAP value types must be RECORD or UNION of RECORD,"
              + " not "
              + unionType.getValueType().getType());
        }
      }
    }
  }

  private static boolean typeMatchesElement(Schema type, XmlSchemaElement element) {
    boolean match = false;

    if (type.getName().equals( element.getName() )) {
      // Confirm the namespaces match.
      String ns = element.getQName().getNamespaceURI();
      if ((ns != null) && !ns.isEmpty()) {
        try {
          if (Utils.getAvroNamespaceFor(ns).equals(
                type.getNamespace()))
          {
            // Namespaces match.
            match = true;
          }
        } catch (URISyntaxException e) {
          throw new IllegalStateException(
              "Element \""
              + element.getQName()
              + "\" has a namespace that is not a valid URI.",
              e);
        }
      } else {
        // There is no namespace; auto-match.
        match = true;
      }
    }

    return match;
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
      QName xmlTypeQName,
      Schema avroType) {

    final Schema xmlAvroType =
        Utils.getAvroSchemaFor(xmlType, xmlTypeQName, false);

    if ((avroType != null) && (xmlAvroType == null)) {
      return false;

    } else if ((avroType == null) && (xmlAvroType != null)) {
      return false;

    } else if ((avroType == null) && (xmlAvroType == null)) {
      return true;

    }

    if (xmlIsWritten) {
      return confirmEquivalent(avroType, xmlAvroType);
    } else {
      return confirmEquivalent(xmlAvroType, avroType);
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
      return confirmEquivalent(
          readerType.getElementType(),
          writerType.getElementType());

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
      /* falls through */
    case DOUBLE:
      // DOUBLE, FLOAT, LONG, INT -> DOUBLE
      convertibleFrom.add(Schema.Type.DOUBLE);
      /* falls through */
    case FLOAT:
      // FLOAT, LONG, INT -> FLOAT
      convertibleFrom.add(Schema.Type.FLOAT);
      /* falls through */
    case LONG:
      // LONG, INT -> LONG
      convertibleFrom.add(Schema.Type.LONG);
      /* falls through */
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
    case RECORD:
      // These are more complex.
      break;

    default:
      throw new IllegalArgumentException(
          "Cannot confirm the equivalency of a reader of type "
          + readerType.getType()
          + " and a writer of type "
          + writerType.getType());
    }

    if ( !convertibleFrom.isEmpty() ) {
      conversionCache.put(writerType.getType(), convertibleFrom);
      return convertibleFrom.contains( readerType.getType() );
    }

    /* If we're here, it's because the writer is either an ENUM or a RECORD.
     * For ENUMs, confirm the writer elements are a superset of the reader
     * elements.  For RECORDs, confirm the fields are convertible. 
     */
    if (writerType.getType().equals(Schema.Type.ENUM)
        && readerType.getType().equals(Schema.Type.ENUM) ) {

      final List<String> writerSymbols = writerType.getEnumSymbols();
      final List<String> readerSymbols = readerType.getEnumSymbols();

      for (String readerSymbol : readerSymbols) {
        if ( !writerSymbols.contains(readerSymbol) ) {
          return false;
        }
      }

      return true;

    } else if (
        writerType.getType().equals(Schema.Type.RECORD)
        && readerType.getType().equals(Schema.Type.RECORD) ) {

      final List<Schema.Field> writerFields = writerType.getFields();
      final List<Schema.Field> readerFields = readerType.getFields();

      if (readerFields.size() == writerFields.size()) {
        boolean equivalent = true;

        for (int fieldIdx = 0; fieldIdx < writerFields.size(); ++fieldIdx) {
          equivalent =
              confirmEquivalent(
                  readerFields.get(fieldIdx).schema(),
                  writerFields.get(fieldIdx).schema());
          if (!equivalent) {
            break;
          }
        }

        return equivalent;
      }

    }

    return false;
  }

  /**
   * Avro maps are tricky because they must be defined all at once, but
   * depending on the schema, their elements may be scattered all across
   * the document.
   *
   * This implementation looks for map nodes that are clustered together,
   * and counts them for when {@link XmlDatumWriter} takes over.  A cluster
   * starts the first time we reach a path node whose underlying Avro schema
   * is of type {@link Schema.Type#MAP}.  A cluster ends when the next
   * traversal out of a map node is to its parent element.  (Intermediary
   * groups do not count as the end of the cluster.)
   *
   * @param path The path to check if is a map node.
   */
  private static void findMaps(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path) {

    Map<QName, List<List<AvroPathNode>>> occurrencesByName =
        new HashMap<QName, List<List<AvroPathNode>>>();

    final ArrayList<StackEntry> docNodeStack =
        new ArrayList<StackEntry>();

    AvroPathNode mostRecentlyLeftMap = null;

    while(path != null) {

      final boolean isElement =
          path
            .getStateMachineNode()
            .getNodeType()
            .equals(XmlSchemaStateMachineNode.Type.ELEMENT);

      final AvroRecordInfo record =
          path.getDocumentNode().getUserDefinedContent();

      final boolean isMapNode =
          (record != null)
          && record.getAvroSchema().getType().equals(Schema.Type.MAP);

      switch (path.getDirection()) {
      case SIBLING:
        {
          if (isElement) {
            /* This is an element increasing its own occurrence.
             * This means we need to pop the previous element off
             * of the stack and start a new one.
             */
            final StackEntry stackEntry =
                docNodeStack.remove(docNodeStack.size() - 1);

            if (mostRecentlyLeftMap != null) {
              addEndNode(occurrencesByName, mostRecentlyLeftMap);
            }

            mostRecentlyLeftMap = null;

            if (stackEntry
                  .docNode
                  .getUserDefinedContent()
                  .getAvroSchema()
                  .getType().equals(Schema.Type.MAP) ) {

              mostRecentlyLeftMap =
                  new AvroPathNode(
                      path,
                      AvroPathNode.Type.MAP_END,
                      stackEntry
                        .docNode
                        .getStateMachineNode()
                        .getElement()
                        .getQName(),
                      stackEntry.occurrence);
            }
          }
        }
        /* falls through */
      case CHILD:
        {
          if (isElement) {
            StackEntry entry = new StackEntry(path.getDocumentNode());

            if (isMapNode) {
              final QName currQName =
                  path
                    .getStateMachineNode()
                    .getElement()
                    .getQName();

              List<List<AvroPathNode>> occurrences = null;
              if ((mostRecentlyLeftMap == null)
                  || !currQName.equals( mostRecentlyLeftMap.getQName() )) {

                if (mostRecentlyLeftMap != null) {
                  addEndNode(occurrencesByName, mostRecentlyLeftMap);
                }

                final ArrayList<AvroPathNode> pathIndices =
                    new ArrayList<AvroPathNode>();
                pathIndices.add(
                    new AvroPathNode(
                        path,
                        AvroPathNode.Type.MAP_START));
                incrementMapParentChildCount(path);

                if (!occurrencesByName.containsKey(currQName)) {
                  occurrences = new ArrayList<List<AvroPathNode>>();
                  occurrencesByName.put(currQName, occurrences);
                } else {
                  occurrences = occurrencesByName.get(currQName);
                }
                occurrences.add(pathIndices);
              } else {
                occurrences = occurrencesByName.get(currQName);
                occurrences
                  .get(occurrences.size() - 1)
                  .add(
                      new AvroPathNode(
                          path,
                          AvroPathNode.Type.ITEM_START));
              }

              entry.occurrence = occurrences.size() - 1;
              mostRecentlyLeftMap = null;
            }

            docNodeStack.add(entry);
          }
          break;
        }
      case PARENT:
        {
          final StackEntry stackEntry =
              docNodeStack.get(docNodeStack.size() - 1);

          if (stackEntry.parentNode == path.getDocumentNode()) {
            docNodeStack.remove(docNodeStack.size() - 1);

            if (mostRecentlyLeftMap != null) {
              addEndNode(occurrencesByName, mostRecentlyLeftMap);
            }

            mostRecentlyLeftMap = null;
            if (stackEntry
                  .docNode
                  .getUserDefinedContent()
                  .getAvroSchema()
                  .getType().equals(Schema.Type.MAP) ) {

              mostRecentlyLeftMap =
                  new AvroPathNode(
                      path,
                      AvroPathNode.Type.MAP_END,
                      stackEntry
                        .docNode
                        .getStateMachineNode()
                        .getElement()
                        .getQName(),
                      stackEntry.occurrence);
            }
          }
          break;
        }
      case CONTENT:
        break;
      default:
        throw new IllegalStateException(
            "Path of "
            + path.getStateMachineNode()
            + " has an unrecognized direction of "
            + path.getDirection()
            + ".");
      }

      path = path.getNext();
    }

    /* Will be 1 if the root is an element,
     * and 0 if the root is a substitution group.
     */
    if (docNodeStack.size() > 1) {
      throw new IllegalStateException(
          "Expected the stack to have no more than one "
          + "element in it at the end, but found "
          + docNodeStack.size()
          + ".");
    }

    for (Map.Entry<QName, List<List<AvroPathNode>>> entry :
           occurrencesByName.entrySet()) {
      for (List<AvroPathNode> avroMapNodes : entry.getValue()) {
        // The MAP_END node doesn't count as a child.
        avroMapNodes.get(0).setMapSize(avroMapNodes.size() - 1);
        for (AvroPathNode avroMapNode : avroMapNodes) {
          avroMapNode.getPathNode().setUserDefinedContent(avroMapNode);
        }
      }
    }
  }

  private static void addEndNode(
      Map<QName, List<List<AvroPathNode>>> occurrencesByName,
      AvroPathNode mostRecentlyLeftMap) {

    final List<List<AvroPathNode>> occurrences =
        occurrencesByName.get(mostRecentlyLeftMap.getQName());
    final List<AvroPathNode> nodes =
        occurrences.get(mostRecentlyLeftMap.getOccurrence());
    nodes.add(mostRecentlyLeftMap);
  }

  /* All of the elements in a map are grouped together, and likewise cannot be
   * counted as part of the MAP's parent's children.  Likewise, each time we
   * find a new MAP, we only increment the parent's child count by one.
   */
  private static void incrementMapParentChildCount(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path) {

    if (!path.getStateMachineNode()
                .getNodeType()
                .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalArgumentException(
          "Starting node should be at an element, not a "
          + path.getStateMachineNode().getNodeType()
          + '.');
    }

    XmlSchemaDocumentNode<AvroRecordInfo> docNode = path.getDocumentNode();
    do {
      docNode = docNode.getParent();
    } while (!docNode
                .getStateMachineNode()
                .getNodeType()
                .equals(XmlSchemaStateMachineNode.Type.ELEMENT));

    if (docNode.getUserDefinedContent() == null) {
      throw new IllegalStateException(
          "Reached a node representing "
          + docNode.getStateMachineNode()
          + ", but it contains no Avro record information.");
    }

    docNode.getUserDefinedContent().incrementChildCount();
  }

  private static void applyContent(
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> startNode) {

    XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path = startNode;

    final ArrayList<StackEntry> docNodeStack =
        new ArrayList<StackEntry>();

    while (path != null) {
      final boolean isElement =
          path
            .getStateMachineNode()
            .getNodeType()
            .equals(XmlSchemaStateMachineNode.Type.ELEMENT);

      switch(path.getDirection()) {
      case SIBLING:
        if (isElement) {
          /* This is an element increasing its own occurrence.
           * This means we need to pop the previous element off
           * of the stack and start a new one.
           */
          docNodeStack.remove(docNodeStack.size() - 1);
        }
        /* falls through */
      case CHILD:
        if (isElement) {
          StackEntry entry = new StackEntry(path.getDocumentNode());
          docNodeStack.add(entry);
        }
        break;
      case PARENT:
        {
          final StackEntry stackEntry =
              docNodeStack.get(docNodeStack.size() - 1);

          if (stackEntry.parentNode == path.getDocumentNode()) {
            docNodeStack.remove(docNodeStack.size() - 1);
          }
          break;
        }
      case CONTENT:
        {
          final StackEntry entry = docNodeStack.get(docNodeStack.size() - 1);
          final AvroRecordInfo recordInfo =
              entry.docNode.getUserDefinedContent();

          Schema schema = recordInfo.getAvroSchema();
          if (schema.getType().equals(Schema.Type.MAP)) {
            schema = schema.getValueType();
            if (recordInfo.getMapUnionIndex() >= 0) {
              schema = schema.getTypes().get(recordInfo.getMapUnionIndex());
            }
          }

          final XmlSchemaElement elem =
              entry.docNode.getStateMachineNode().getElement();

          final XmlSchemaTypeInfo elemType =
              entry.docNode.getStateMachineNode().getElementType();

          final Schema.Field childField =
              schema.getField(elem.getQName().getLocalPart());

          if (elemType.isMixed() && (childField != null)) {
            schema = childField.schema();
            int unionIdx = -1;
            if (schema.getType().equals(Schema.Type.ARRAY)
                && schema
                     .getElementType()
                     .getType()
                     .equals(Schema.Type.UNION)) {
              final List<Schema> unionTypes =
                  schema.getElementType().getTypes();

              for (unionIdx = 0; unionIdx < unionTypes.size(); ++unionIdx) {
                if (unionTypes
                      .get(unionIdx)
                      .getType()
                      .equals(Schema.Type.STRING)) {
                  break;
                }
              }
              if (unionIdx == unionTypes.size()) {
                throw new IllegalStateException(
                    "Element "
                    + elem.getQName()
                    + " is a mixed type, but its internal"
                    + " union does not have a STRING!");
              }
              recordInfo.incrementChildCount();

              final AvroPathNode pathNode = path.getUserDefinedContent();
              if (pathNode == null) {
                path.setUserDefinedContent(new AvroPathNode(unionIdx));
              } else {
                throw new IllegalStateException(
                    "The path node is for CONTENT, but an "
                    + "AvroPathNode already exists!");
              }
            }
          }

          break;
        }
      default:
        throw new IllegalStateException(
            "Path of "
            + path.getStateMachineNode()
            + " has an unrecognized direction of "
            + path.getDirection()
            + ".");
      }

      path = path.getNext();
    }
  }
}
