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
import java.util.SortedMap;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaUse;

/**
 * Applies an Avro schema to a tree described
 * by {@link XmlSchemaDocumentNode}s.
 *
 * @author  Mike Pigott
 */
final class AvroSchemaApplier {

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

      unionOfValidElementsStack.add(avroSchema.getElementType());

    } else if ( avroSchema.getType().equals(Schema.Type.UNION) ) {
      /* It is possible for the root element to actually be the root of a
       * substitution group.  If this happens, the root element could be
       * one of many different record types.
       *
       * This can only be valid if the schema is a union of records.
       */
      verifyIsUnionOfMapsAndRecords(avroSchema);

      unionOfValidElementsStack.add(avroSchema);

    } else if ( avroSchema.getType().equals(Schema.Type.RECORD)
        || avroSchema.getType().equals(Schema.Type.MAP) ) {
      // This is a definition of the root element.
      List<Schema> union = new ArrayList<Schema>(1);
      union.add(avroSchema);
      unionOfValidElementsStack.add( Schema.createUnion(union) );

    } else {
      throw new IllegalArgumentException("The Avro Schema must be one of the following types: RECORD, MAP, UNION of RECORDs/MAPs, or ARRAY of UNION of RECORDs/MAPs.");
    }
  }

  void apply(XmlSchemaDocumentNode<AvroRecordInfo> docNode) {
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
      throw new IllegalArgumentException("Document node has an unrecognized type of " + docNode.getStateMachineNode().getNodeType() + '.');
    }
  }

  private void processElement(XmlSchemaDocumentNode<AvroRecordInfo> doc) {
    if (!doc
           .getStateMachineNode()
           .getNodeType()
           .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalStateException("Attempted to process an element when the node type is " + doc.getStateMachineNode().getNodeType());
    }

    final XmlSchemaElement element = doc.getStateMachineNode().getElement();

    final List<Schema> validNextElements =
        unionOfValidElementsStack
          .get(unionOfValidElementsStack.size() - 1)
          .getTypes();

    Schema elemSchema = null;
    int schemaIndex = 0;
    if (validNextElements != null) {
      for (; schemaIndex < validNextElements.size(); ++schemaIndex) {
        Schema possibleSchema = validNextElements.get(schemaIndex);
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
            attribute.getType());
      }

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
            if (typeInfo.getType().equals(XmlSchemaTypeInfo.Type.LIST)) {
              break;
            }

            // All group types are ARRAY of UNION of MAP/RECORD.
            if ( !childrenSchema
                    .getElementType()
                    .getType()
                    .equals(Schema.Type.UNION) ) {
              throw new IllegalStateException("If the children of " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName() + " are in a group, the corresponding Avro Schema MUST BE an ARRAY of UNION of MAPs/RECORDs, not " + childrenSchema.getElementType().getType());
            }

            verifyIsUnionOfMapsAndRecords( childrenSchema.getElementType() );

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
          {
            if (!confirmEquivalent(
                    typeInfo,
                    element.getQName(),
                    childrenSchema) ) {
              throw new IllegalStateException("Cannot convert between " + typeInfo + " and " + childrenSchema + " for simple content of " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName());
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
          throw new IllegalStateException("Children of element " + element.getQName() + " in Avro Schema {" + elemSchema.getNamespace() + "}" + elemSchema.getName() + " must be either an ARRAY of UNION of MAP/RECORD or a primitive type, not " + childrenSchema.getType());
        }
      }

      AvroRecordInfo recordInfo = null;
      if ( avroRecordStack.isEmpty() ) {
        recordInfo = new AvroRecordInfo(elemSchema);
        avroRecordStack.add(recordInfo);
      } else {
        recordInfo = new AvroRecordInfo(elemSchema, schemaIndex);
        avroRecordStack.get(avroRecordStack.size() - 1).incrementChildCount();
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
      XmlSchemaTypeInfo attributeType) {

    Schema valueType = elementSchema;
    if ( valueType.getType().equals(Schema.Type.MAP) ) {
      valueType = valueType.getValueType();
    }

    final Schema.Field attrField = valueType.getField( attribute.getName() );

    if (xmlIsWritten
        && (attrField == null)
        && !attribute.getUse().equals(XmlSchemaUse.OPTIONAL)
        && !attribute.getUse().equals(XmlSchemaUse.PROHIBITED)) {
      throw new IllegalStateException("Element " + elementName + " has a " + attribute.getUse() + " attribute named " + attribute.getQName() + " - when writing to XML, a field in the Avro record must exist.");
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
        throw new IllegalStateException("Cannot convert element " + elementName + " attribute " + attribute.getQName() + " types between " + attributeType.getBaseType() + " and " + attrField.schema());
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
      throw new IllegalStateException("Attempted to process a group, but the document node is of type " + doc.getStateMachineNode().getNodeType());
    }
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

  private List<Schema> unionOfValidElementsStack;
  private List<AvroRecordInfo> avroRecordStack;

  private final Schema avroSchema;
  private final Map<Schema.Type, Set<Schema.Type>> conversionCache;
  private final boolean xmlIsWritten;
}
