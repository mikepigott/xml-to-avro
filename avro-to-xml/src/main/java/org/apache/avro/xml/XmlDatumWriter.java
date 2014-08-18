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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.DatatypeConverter;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.constants.Constants;
import org.w3c.dom.Document;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Reads an XML {@link Document} and writes it to an {@link Encoder}.
 * <p>
 * Generates an Avro {@link Schema} on the fly from the XML Schema itself. 
 * That {@link Schema} can be retrieved by calling {@link #getSchema()}.
 * </p>
 */
public class XmlDatumWriter implements DatumWriter<Document> {

  private static final QName NIL_ATTR =
      new QName("http://www.w3.org/2001/XMLSchema-instance", "nil");

  private final XmlSchemaCollection xmlSchemaCollection;
  private final XmlSchemaStateMachineNode stateMachine;
  private Schema schema;

  private static class StackEntry {
    XmlSchemaDocumentNode<AvroRecordInfo> docNode;
    boolean receivedContent;

    StackEntry(XmlSchemaDocumentNode<AvroRecordInfo> docNode) {
      this.docNode = docNode;
      this.receivedContent = false;
    }
  }

  private static class Writer extends DefaultHandler {
    private static final XmlSchemaTypeInfo XML_MIXED_CONTENT_TYPE =
        new XmlSchemaTypeInfo(XmlSchemaBaseSimpleType.STRING);

    private static final Schema AVRO_MIXED_CONTENT_SCHEMA =
        Schema.create(Schema.Type.STRING);

    private XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> currLocation;
    private StringBuilder content;
    private QName currAnyElem;
    private ArrayList<StackEntry> stack;

    private final XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path;
    private final Encoder out;
    private final XmlSchemaNamespaceContext nsContext;

    Writer(
        XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path,
        Encoder out) {

      this.path = path;
      this.out = out;

      nsContext = new XmlSchemaNamespaceContext();
      stack = new ArrayList<StackEntry>();
      currLocation = null;
      content = null;
      currAnyElem = null;
    }

    @Override
    public void startDocument() throws SAXException {
      currLocation = path;
    }

    @Override
    public void startPrefixMapping(String prefix, String uri)
        throws SAXException {

      nsContext.addNamespace(prefix, uri);
    }

    @Override
    public void endPrefixMapping(String prefix) throws SAXException {
      nsContext.removeNamespace(prefix);
    }

    @Override
    public void startElement(
        String uri,
        String localName,
        String qName,
        Attributes atts) throws SAXException {

      if (currAnyElem != null) {
        // We are inside an any element and not processing this one.
        return;
      }

      final QName elemQName = new QName(uri, localName);
      walkToElement(elemQName);

      if ( elemQName.getLocalPart().equals("anyAndFriends") ) {
        //log = true;
      }

      if (!currLocation
            .getDirection()
            .equals(XmlSchemaPathNode.Direction.CHILD)
          && !currLocation
               .getDirection()
               .equals(XmlSchemaPathNode.Direction.SIBLING)) {
        throw new IllegalStateException(
            "We are starting an element, so our path node direction should be "
            + "to a CHILD or SIBLING, not "
            + currLocation.getDirection());
      }

      if (currLocation
            .getStateMachineNode()
            .getNodeType()
            .equals(XmlSchemaStateMachineNode.Type.ANY)) {

        // This is an any element; we are not processing it.
        currAnyElem = elemQName;
        return;
      }

      try {
        final XmlSchemaDocumentNode<AvroRecordInfo> doc =
            currLocation.getDocumentNode();
        final AvroRecordInfo recordInfo = doc.getUserDefinedContent();

        Schema avroSchema = recordInfo.getAvroSchema();

        final List<XmlSchemaStateMachineNode.Attribute> attributes =
            doc.getStateMachineNode().getAttributes();

        final HashMap<String, XmlSchemaTypeInfo> attrTypes =
            new HashMap<String, XmlSchemaTypeInfo>();

        final HashMap<String, XmlSchemaAttribute> schemaAttrs =
            new HashMap<String, XmlSchemaAttribute>();

        for (XmlSchemaStateMachineNode.Attribute attribute : attributes) {
          attrTypes.put(
              attribute.getAttribute().getName(),
              attribute.getType());

          schemaAttrs.put(
              attribute.getAttribute().getName(),
              attribute.getAttribute());
        }

        // If there are children, we want to start an array and end it later.
        final StackEntry entry =
            new StackEntry(currLocation.getDocumentNode());

        if (avroSchema.getType().equals(Schema.Type.RECORD)) {
          if ( !stack.isEmpty() ) {
            out.startItem();
          }
          if (recordInfo.getUnionIndex() >= 0) {
            out.writeIndex( recordInfo.getUnionIndex() );
          }

        } else if ( avroSchema.getType().equals(Schema.Type.MAP) ) {
          final AvroPathNode mapNode = currLocation.getUserDefinedContent();
          if (mapNode == null) {
            throw new IllegalStateException(
                "Reached "
                + elemQName
                + ", a MAP node, but there is no map information here.");
          }

          switch ( mapNode.getType() ) {
          case MAP_START:
            {
              if ( !stack.isEmpty() ) {
                out.startItem();
              }
              if (recordInfo.getUnionIndex() >= 0) {
                out.writeIndex( recordInfo.getUnionIndex() );
              }
              out.writeMapStart();
              out.setItemCount( mapNode.getMapSize() );
            }
          case ITEM_START:
            {
              out.startItem();

              avroSchema = avroSchema.getValueType();

              /* If the MAP value is another UNION, reach
               * into that one to fetch the schema.
               */
              final int mapUnionIndex = recordInfo.getMapUnionIndex();
              if (mapUnionIndex >= 0) {
                avroSchema = avroSchema.getTypes().get(mapUnionIndex);
              }

              String key = null;

              for (int fieldIndex = 0;
                  fieldIndex < avroSchema.getFields().size() - 1;
                  ++fieldIndex) {

                final Schema.Field field =
                    avroSchema.getFields().get(fieldIndex);

                final XmlSchemaTypeInfo attrType = attrTypes.get(field.name());

                final XmlSchemaAttribute xsa = schemaAttrs.get(field.name());

                if ((attrType.getUserRecognizedType() != null)
                    && attrType
                         .getUserRecognizedType()
                         .equals(Constants.XSD_ID)) {
                  key =
                      getAttrValue(
                          atts,
                          xsa.getQName().getNamespaceURI(),
                          field.name());

                  if (key == null) {
                    throw new IllegalStateException(
                        "Attribute value for "
                        + xsa.getQName()
                        + " of element "
                        + elemQName
                        + " is null.");
                  }
                  break;
                }
              }

              if (key == null) {
                throw new IllegalStateException(
                    "Unable to find key for element " + elemQName);
              }

              out.writeString(key);

              /* If the MAP value is another UNION, write
               * the union index before continuing.
               */
              if (mapUnionIndex >= 0) {
                out.writeIndex(mapUnionIndex);
              }
              break;
            }
          case MAP_END:
          case CONTENT:
          default:
            throw new IllegalStateException(
                "Did not expect to find a map node of type "
                + mapNode.getType()
                + " when starting "
                + elemQName
                + ".");
          }

        } else {
          throw new IllegalStateException(
              "Elements are either MAPs or RECORDs, not "
              + avroSchema.getType()
              + "s.");
        }

        /* The last element in the set of fields is the children.  We want
         * to process the children separately as they require future calls
         * to characters() and/or startElement().
         */
        for (int fieldIndex = 0;
            fieldIndex < avroSchema.getFields().size() - 1;
            ++fieldIndex) {

          final Schema.Field field = avroSchema.getFields().get(fieldIndex);
          if (field.name().equals(elemQName.getLocalPart())) {
            // We reached the children field early ... not supposed to happen!
            throw new IllegalStateException(
                "The children field is indexed at "
                + fieldIndex
                + " when it was expected to be the last element, or "
                + (avroSchema.getFields().size() - 1)
                + ".");
          }

          final XmlSchemaTypeInfo typeInfo = attrTypes.get( field.name() );
          final QName attrQName = schemaAttrs.get( field.name() ).getQName();

          String value =
              getAttrValue(
                  atts,
                  attrQName.getNamespaceURI(),
                  field.name());

          if (value == null) {
            // See if there is a default or fixed value instead.
            final XmlSchemaAttribute schemaAttr =
                schemaAttrs.get( field.name() );

            value = schemaAttr.getDefaultValue();
            if (value == null) {
              value = schemaAttr.getFixedValue();
            }
          }

          try {
            write(typeInfo, attrQName, field.schema(), value);
          } catch (Exception e) {
            throw new RuntimeException(
                "Could not write "
                + field.name()
                + " in "
                + field.schema().toString()
                + " to the output stream for element "
                + elemQName,
                e);
          }
        }

        final XmlSchemaTypeInfo elemType =
            doc.getStateMachineNode().getElementType();

        boolean isComplexType = true;
        if ( !elemType.getType().equals(XmlSchemaTypeInfo.Type.COMPLEX) ) {
          isComplexType = false;
        }

        if (avroSchema
              .getField( elemQName.getLocalPart() )
              .schema()
              .getType()
              .equals(Schema.Type.ARRAY)
            && isComplexType) {
          out.writeArrayStart();

          if (recordInfo.getNumChildren() > 0) {
            out.setItemCount( recordInfo.getNumChildren() );
          } else {
            out.setItemCount(0);
          }

          /* We expect to receive child elements; no need to look
           * for a default or fixed value once this element exits.
           */
          entry.receivedContent = true;

        } else if (avroSchema
                     .getField( elemQName.getLocalPart() )
                     .schema()
                     .getType()
                     .equals(Schema.Type.NULL) ) {
          out.writeNull();
          entry.receivedContent = true;

        } else {
          final int nilIndex =
              atts.getIndex(
                  NIL_ATTR.getNamespaceURI(),
                  NIL_ATTR.getLocalPart()); 

          if ((nilIndex >= 0)
              && Boolean.parseBoolean(atts.getValue(nilIndex))) {

            write(doc.getStateMachineNode().getElementType(),
                  elemQName,
                  avroSchema.getField( elemQName.getLocalPart() ).schema(),
                  null);
            entry.receivedContent = true;
          }
        }

        stack.add(entry);

      } catch (Exception e) {
        throw new RuntimeException(
            "Unable to write "
            + elemQName
            + " to the output stream.",
            e);
      }
    }

    @Override
    public void characters(char[] ch, int start, int length)
        throws SAXException {

      if (currAnyElem != null) {
        // We do not process wildcard elements.
        return;
      }

      if (stack.isEmpty()) {
        throw new SAXException(
            "We are processing content, but the element stack is empty!");
      }

      final XmlSchemaDocumentNode<AvroRecordInfo> owningElem =
          stack.get(stack.size() - 1).docNode;

      XmlSchemaPathNode path = walkToContent(owningElem);

      if (path == null) {
        final String str = new String(ch, start, length).trim();

        if (str.isEmpty()) {
          return;
        } else {
          if (path == null) {
            throw new SAXException(
                "We are processing characters \""
                + str
                + "\" for "
                + owningElem
                    .getStateMachineNode()
                    .getElement()
                    .getQName()
                + " but the current direction is "
                + currLocation.getDirection()
                + " to "
                + currLocation.getStateMachineNode()
                + ", not CONTENT.");
          }
        }

      } else {
        currLocation = path;

        if (currLocation.getNext() == null) {
          throw new SAXException(
              "We are processing characters for "
              + stack.get(stack.size() - 1)
                  .docNode
                  .getStateMachineNode()
                  .getElement()
                  .getQName()
              + " but somehow the path ends here!");
        }
      }

      /* If characters() will be called multiple times, we want to collect
       * all of them in the "content" StringBuilder, then process it all
       * once the last bit of content has been collected.
       *
       * This includes where content is interspersed with any elements, which
       * are skipped anyway.
       *
       * If this is the last content node, we'll just write it all out here.
       */
      final boolean moreContentComing =
          hasMoreContent(currLocation.getNext(), owningElem);

      String result = null;
      if (moreContentComing
          || ((content != null) && (content.length() > 0))) {

        if (content == null) {
          content = new StringBuilder();
        }
        content.append(ch, start, length);

        if (!moreContentComing) {
          // If this is the last node, process the content.
          result = content.toString();
          content.delete(0, content.length());
        }
      } else {
        // This is the only content node - just write it.
        result = new String(ch, start, length);
      }

      if (result != null) {
        final StackEntry entry = stack.get(stack.size() - 1);
        final XmlSchemaDocumentNode<AvroRecordInfo> docNode = entry.docNode;

        final XmlSchemaTypeInfo elemType =
            docNode.getStateMachineNode().getElementType();

        final QName elemQName =
            docNode
              .getStateMachineNode()
              .getElement()
              .getQName();

        final Schema avroSchema =
           docNode
             .getUserDefinedContent()
             .getAvroSchema()
             .getField(elemQName.getLocalPart())
             .schema();

        try {
          final AvroPathNode contentPathNode =
              currLocation.getUserDefinedContent();

          if ((contentPathNode != null)
              && contentPathNode.getType().equals(AvroPathNode.Type.CONTENT)) {

            out.startItem();
            out.writeIndex(contentPathNode.getContentUnionIndex());

            write(
                XML_MIXED_CONTENT_TYPE,
                elemQName,
                AVRO_MIXED_CONTENT_SCHEMA,
                result);

          } else {
            write(elemType, elemQName, avroSchema, result);
          }
          entry.receivedContent = true;
        } catch (Exception e) {
          throw new RuntimeException(
              "Unable to write the content \""
              + result
              + "\" for "
              + elemQName,
              e);
        }
      }
    }

    @Override
    public void endElement(
        String uri,
        String localName,
        String qName)
        throws SAXException
    {
      final QName elemQName = new QName(uri, localName);

      if (currAnyElem != null) {
        if (currAnyElem.equals(elemQName)) {
          // We are exiting an any element; prepare for the next one!
          currAnyElem = null;
        }
        return;
      }

      final StackEntry entry = stack.remove(stack.size() - 1);
      final XmlSchemaDocumentNode<AvroRecordInfo> docNode = entry.docNode;

      final XmlSchemaTypeInfo elemType =
          docNode.getStateMachineNode().getElementType();

      if (!entry.receivedContent) {

        /* Look for either the default value
         * or fixed value and apply it, if any.
         */
        String value =
            docNode.getStateMachineNode().getElement().getDefaultValue();

        if (value == null) {
          value = docNode.getStateMachineNode().getElement().getFixedValue();
        }

        final AvroRecordInfo record = docNode.getUserDefinedContent();

        Schema avroSchema = record.getAvroSchema();

        if ( avroSchema.getType().equals(Schema.Type.MAP) ) {
          avroSchema = avroSchema.getValueType();

          if (record.getMapUnionIndex() >= 0) {
            avroSchema = avroSchema.getTypes().get(record.getMapUnionIndex());
          }
        }

        avroSchema = avroSchema.getField(localName).schema();

        try {
          write(elemType, elemQName, avroSchema, value);
        } catch (IOException e) {
          throw new RuntimeException(
              "Attempted to write a default value of \""
              + value
              + "\" for "
              + elemQName
              + " and failed.",
              e);
        }
      }

      final QName stackElemQName =
          docNode
            .getStateMachineNode()
            .getElement()
            .getQName();

      if (!stackElemQName.equals(elemQName)) {
        throw new IllegalStateException(
            "We are leaving "
            + elemQName
            + " but the element on the stack is "
            + stackElemQName + ".");
      }

      Schema avroSchema =
          docNode
            .getUserDefinedContent()
            .getAvroSchema();

      boolean isMapEnd = false;
      if (avroSchema.getType().equals(Schema.Type.MAP)) {
        avroSchema = avroSchema.getValueType();

        final int mapUnionIndex =
            docNode.getUserDefinedContent().getMapUnionIndex();
        if (mapUnionIndex >= 0) {
          avroSchema = avroSchema.getTypes().get(mapUnionIndex);
        }

        isMapEnd = isMapEnd();
      }

      boolean isComplexType = true;
      if ( !elemType.getType().equals(XmlSchemaTypeInfo.Type.COMPLEX) ) {
        isComplexType = false;
      }

      if (avroSchema
            .getField( elemQName.getLocalPart() )
            .schema()
            .getType()
            .equals(Schema.Type.ARRAY)
          && isComplexType) {
        try {
          out.writeArrayEnd();
        } catch (Exception e) {
          throw new RuntimeException(
              "Unable to end the array for " + elemQName, e);
        }
      }

      if (isMapEnd) {
        try {
          out.writeMapEnd();
        } catch (Exception e) {
          throw new RuntimeException("Unable to process a MAP_END.", e);
        }

      }
    }

    @Override
    public void endDocument() throws SAXException {
      if (currLocation.getNext() != null) {
        currLocation = currLocation.getNext();
        while (currLocation != null) {
          if (!currLocation
                 .getDirection()
                 .equals(XmlSchemaPathNode.Direction.PARENT)) {
            throw new IllegalStateException(
                "Path has more nodes after document end: "
                + currLocation.getDirection()
                + " | "
                + currLocation.getStateMachineNode());
          }
          currLocation = currLocation.getNext();
        }
      }
    }

    private void walkToElement(QName elemName) {
      if (stack.isEmpty()
          && currLocation
               .getStateMachineNode()
               .getNodeType()
               .equals(XmlSchemaStateMachineNode.Type.ELEMENT)
          && currLocation
               .getStateMachineNode()
               .getElement()
               .getQName()
               .equals(elemName)) {
        return;
      }

      do {
        currLocation = currLocation.getNext();
      } while ((currLocation != null)
                && (currLocation
                      .getDirection()
                      .equals(XmlSchemaPathNode.Direction.PARENT)
                    || (!currLocation
                           .getDirection()
                           .equals(XmlSchemaPathNode.Direction.PARENT)
                         && !currLocation
                               .getStateMachineNode()
                               .getNodeType()
                               .equals(XmlSchemaStateMachineNode.Type.ELEMENT)
                         && !currLocation
                               .getStateMachineNode()
                               .getNodeType()
                               .equals(XmlSchemaStateMachineNode.Type.ANY))));

      if (currLocation == null) {
        throw new IllegalStateException(
            "Cannot find " + elemName + " in the path!");

      } else if (
          currLocation
            .getStateMachineNode()
            .getNodeType()
            .equals(XmlSchemaStateMachineNode.Type.ELEMENT)
          && !currLocation
                .getStateMachineNode()
                .getElement()
                .getQName()
                .equals(elemName)) {
        throw new IllegalStateException(
            "The next element in the path is "
            + currLocation.getStateMachineNode().getElement().getQName()
            + " ("
            + currLocation.getDirection()
            + "), not "
            + elemName
            + ".");
      }
    }

    /**
     * For a path to be exiting a particular element's
     * scope, it must be doing one of four things:
     *
     * 1. It is null, indicating the end of the document.
     * 2. It is a PARENT path to the owning element's parent.
     * 3. It is a CHILD path to the owning element's child (wildcard) element.
     * 4. It is a SIBLING path to a new element instance.
     */
    private static boolean pathExitsElementScope(
        XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path,
        XmlSchemaDocumentNode<AvroRecordInfo> owningElem,
        boolean ignoreAny) {

      // 1. This is the end of the path.
      if (path == null) {
        return true;
      }

      // 2. This is a PARENT path to the owning element's parent.
      final XmlSchemaDocumentNode<AvroRecordInfo> parentElem =
          owningElem.getParent();

      if (path.getDirection().equals(XmlSchemaPathNode.Direction.PARENT)
          && path.getDocumentNode() == parentElem) {
        return true;
      }

      // 3. It is a CHILD path to the owning element's child.
      final XmlSchemaStateMachineNode.Type nodeType =
          path.getStateMachineNode().getNodeType();

      final boolean isElement =
          nodeType.equals(XmlSchemaStateMachineNode.Type.ELEMENT);

      final boolean isAny =
          nodeType.equals(XmlSchemaStateMachineNode.Type.ANY);

      if (path.getDirection().equals(XmlSchemaPathNode.Direction.CHILD)
          && (isElement || (isAny && !ignoreAny))) {
        return true;
      }

      // 4. It is a SIBLING path to a new element instance.
      if (path.getDirection().equals(XmlSchemaPathNode.Direction.SIBLING)
          && (isElement || (isAny && !ignoreAny))) {
        return true;
      }

      // It is none of these things; we are still in the scope.
      return false;
    }

    private static boolean hasMoreContent(
        XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path,
        XmlSchemaDocumentNode<AvroRecordInfo> owningElem) {

      if (path == null) {
        return false;
      }

      while (!pathExitsElementScope(path, owningElem, true)
             && !path
                  .getDirection()
                  .equals(XmlSchemaPathNode.Direction.CONTENT) ) {
        path = path.getNext();
      }

      if ((path != null)
          && path
               .getDirection()
               .equals(XmlSchemaPathNode.Direction.CONTENT) ) {
        return true;
      }

      return false;
    }

    private XmlSchemaPathNode walkToContent(
        XmlSchemaDocumentNode<AvroRecordInfo> owningElem) {

      if (currLocation == null) {
        return null;
      }

      XmlSchemaPathNode path = currLocation.getNext();

      while (!pathExitsElementScope(path, owningElem, false)
             && !path
                  .getDirection()
                  .equals(XmlSchemaPathNode.Direction.CONTENT) ) {
        path = path.getNext();
      }

      if ((path != null)
          && path
               .getDirection()
               .equals(XmlSchemaPathNode.Direction.CONTENT) ) {
        return path;
      }

      return null;
    }

    private boolean isMapEnd() {
      XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> position = currLocation;
      AvroPathNode pathInfo = null;

      do {
        position = position.getNext();
        if (position != null) {
          pathInfo = position.getUserDefinedContent();
        }
      } while ((position != null)
                && ((pathInfo == null)
                    || ((pathInfo != null)
                         && pathInfo.getType().equals(
                             AvroPathNode.Type.CONTENT))));

      return ((position != null)
              && position
                   .getUserDefinedContent()
                   .getType()
                   .equals(AvroPathNode.Type.MAP_END));
    }

    private static String getAttrValue(
        Attributes atts,
        String namespaceUri,
        String name) {

      /* Attributes in XML Schema each have their own namespace, which
       * is not supported in Avro.  So, we will see if we can find the
       * attribute using the existing namespace, and if not, we will
       * walk all of them to see which one has the same name.
       */
      String value =
          atts.getValue(namespaceUri, name);

      if (value == null) {
        for (int attrIndex = 0;
            attrIndex < atts.getLength();
            ++attrIndex) {
          if (atts.getLocalName(attrIndex).equals(name)) {
            value = atts.getValue(attrIndex);
            break;
          }
        }
      }

      return value;
    }

    private void write(
        XmlSchemaTypeInfo xmlType,
        QName xmlQName,
        Schema schema,
        String data) throws IOException {

      write(xmlType, xmlQName, schema, data, -1);
    }

    private void write(
        XmlSchemaTypeInfo xmlType,
        QName xmlQName,
        Schema schema,
        String data,
        int unionIndex)
        throws IOException {

      /* If the data is empty or null, write
       * it as a null or string, if possible.
       */
      final XmlSchemaBaseSimpleType baseType = xmlType.getBaseType();

      if ((data == null) || data.isEmpty()) {
        boolean isNullable = (schema.getType().equals(Schema.Type.NULL));
        boolean isString = (schema.getType().equals(Schema.Type.STRING));
        int nullUnionIndex = -1;
        int stringIndex = -1;
        if (!isNullable
            && !isString
            && schema.getType().equals(Schema.Type.UNION)) {

          for (int typeIndex = 0;
              typeIndex < schema.getTypes().size();
              ++typeIndex) {

            final Schema.Type type =
                schema.getTypes().get(typeIndex).getType();

            if (type.equals(Schema.Type.NULL)) {
              nullUnionIndex = typeIndex;
              isNullable = true;
              break;
            } else if (type.equals(Schema.Type.STRING)) {
              isString = true;
              stringIndex = typeIndex;
            }
          }
        }

        if (isString && (data != null) && data.isEmpty()) {
          // Preserve empty strings when possible.
          if (stringIndex >= 0) {
            out.writeIndex(stringIndex);
          }
          out.writeString(data);

        } else if (isNullable) {
          if (nullUnionIndex >= 0) {
            out.writeIndex(nullUnionIndex);
          }
          out.writeNull();

        } else if (isString) {
          if (stringIndex >= 0) {
            out.writeIndex(stringIndex);
          }
          out.writeString("");

        } else {
            throw new IOException(
                "Cannot write a null or empty string "
                + "as a non-null or non-string type.");
        }

        return;
      }

      switch ( schema.getType() ) {
      case ARRAY:
        {
          /* While unions of lists of different types are technically possible, 
           * supporting them here would be difficult, to say the least.  For
           * now, only one array type will be supported in a union.
           */
          if (unionIndex >= 0) {
            out.writeIndex(unionIndex);
          }

          if ( xmlType.getType().equals(XmlSchemaTypeInfo.Type.UNION) ) {
            xmlType = Utils.chooseUnionType(xmlType, null, schema, unionIndex);
          }
          if ( xmlType.getType().equals(XmlSchemaTypeInfo.Type.LIST) ) {
            xmlType = xmlType.getChildTypes().get(0);
          }

          final String[] items = data.split(" ");
          final List<String> itemList = new ArrayList<String>(items.length);
          for (String item : items) {
            if ( !item.isEmpty() ) {
              itemList.add(item);
            }
          }
          out.writeArrayStart();
          out.setItemCount( itemList.size() );
          for (String item : itemList) {
            out.startItem();
            write(xmlType, xmlQName, schema.getElementType(), item);
          }
          out.writeArrayEnd();
          break;
        }
      case UNION:
        {
          int textIndex = -1;
          int bytesIndex = -1;

          Schema bytesType = null;

          final List<Schema> subTypes = schema.getTypes();
          boolean written = false;
          for (int subTypeIndex = 0;
              subTypeIndex < subTypes.size();
              ++subTypeIndex) {
            // Try the text types last.
            final Schema subType = subTypes.get(subTypeIndex);
            if (subType.getType().equals(Schema.Type.BYTES)) {
              bytesIndex = subTypeIndex;
              bytesType = subType;
              continue;
            } else if (subType.getType().equals(Schema.Type.STRING)) {
              textIndex = subTypeIndex;
              continue;
            }

            // Determine the corresponding XML union type.
            XmlSchemaTypeInfo xmlSubType = xmlType;
            if ( xmlType.getType().equals(XmlSchemaTypeInfo.Type.UNION) ) {
              xmlSubType =
                  Utils.chooseUnionType(
                      xmlType,
                      xmlQName,
                      subType,
                      subTypeIndex);
            }

            if (xmlSubType != null) {
              try {
                write(xmlSubType, xmlQName, subType, data, subTypeIndex);
                written = true;
                break;
              } catch (Exception e) {
                /* Could not parse the value using the
                 * provided type; try the next one.
                 */
              }
            }
          }

          if (!written) {
            if (bytesIndex >= 0) {
              XmlSchemaTypeInfo subType = xmlType;
              if (xmlType.getType().equals(XmlSchemaTypeInfo.Type.UNION)) {
                subType =
                    Utils.chooseUnionType(
                        xmlType,
                        xmlQName,
                        schema.getTypes().get(bytesIndex),
                        bytesIndex);
              }

              // Only write the bytes if we know how.
              if (subType != null) {
                try {
                  write(subType, xmlQName, bytesType, data, bytesIndex);
                  written = true;
                } catch (Exception e) {
                  // Cannot write the data as bytes either.
                }
              }
            }
            if (!written && (textIndex >= 0)) {
              out.writeIndex(textIndex);
              out.writeString(data);

            } else if (!written) {
              throw new IOException(
                  "Cannot write \""
                  + data
                  + "\" as one of the types in "
                  + schema.toString());
            }
          }
          break;
        }
      case BYTES:
        {
          byte[] bytes = null;
          switch (baseType) {
          case BIN_BASE64:
            bytes = DatatypeConverter.parseBase64Binary(data);
            break;
          case BIN_HEX:
            bytes = DatatypeConverter.parseHexBinary(data);
            break;
          case DECIMAL:
            {
              final BigDecimal decimal =
                  Utils.createBigDecimalFrom(data, schema);
              final BigInteger unscaledValue =
                  decimal.unscaledValue();
              bytes = unscaledValue.toByteArray();
              break;
            }
          default:
            throw new IllegalArgumentException(
                "Cannot generate bytes for data of a base type of "
                + baseType);
          }
          if (unionIndex >= 0) {
            out.writeIndex(unionIndex);
          }
          out.writeBytes(bytes);
          break;
        }
      case STRING:
        {
          if (unionIndex >= 0) {
            out.writeIndex(unionIndex);
          }
          out.writeString(data);
          break;
        }
      case ENUM:
        {
          if ( !schema.hasEnumSymbol(data) ) {
            final int numSymbols = schema.getEnumSymbols().size();

            StringBuilder errMsg = new StringBuilder("\"");
            errMsg.append(data);
            errMsg.append("\" is not a member of the symbols [\"");
            for (int symbolIndex = 0;
                symbolIndex < numSymbols - 1;
                ++symbolIndex) {
              errMsg.append( schema.getEnumSymbols().get(symbolIndex) );
              errMsg.append("\", \"");
            }
            errMsg.append( schema.getEnumSymbols().get(numSymbols - 1) );
            errMsg.append("\"].");

            throw new IOException( errMsg.toString() );
          }
          if (unionIndex >= 0) {
            out.writeIndex(unionIndex);
          }
          out.writeEnum( schema.getEnumOrdinal(data) );
          break;
        }
      case DOUBLE:
        {
          try {
            final double value = Double.parseDouble(data);
            if (unionIndex >= 0) {
              out.writeIndex(unionIndex);
            }
            out.writeDouble(value);
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a double.", nfe);
          }
          break;
        }
      case FLOAT:
        {
          try {
            final float value = Float.parseFloat(data);
            if (unionIndex >= 0) {
              out.writeIndex(unionIndex);
            }
            out.writeFloat(value);
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a float.", nfe);
          }
          break;
        }
      case LONG:
        {
          try {
            final long value = Long.parseLong(data);
            if (unionIndex >= 0) {
              out.writeIndex(unionIndex);
            }
            out.writeLong(value);
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a long.", nfe);
          }
          break;
        }
      case INT:
        {
          try {
            final int value = Integer.parseInt(data);
            if (unionIndex >= 0) {
              out.writeIndex(unionIndex);
            }
            out.writeInt(value);
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not an int.", nfe);
          }
          break;
        }
      case BOOLEAN:
        {
          if (data.equalsIgnoreCase("true")
              || data.equalsIgnoreCase("false")) {
            if (unionIndex >= 0) {
              out.writeIndex(unionIndex);
            }
            out.writeBoolean( Boolean.parseBoolean(data) );
          } else {
            throw new IOException('"' + data + "\" is not a boolean.");
          }
          break;
        }
      case RECORD:
        {
          switch (baseType) {
          case QNAME:
            {
              try {
                final QName qName =
                    DatatypeConverter.parseQName(data, nsContext);

                if (unionIndex >= 0) {
                  out.writeIndex(unionIndex);
                }
                out.writeString( qName.getNamespaceURI() );
                out.writeString( qName.getLocalPart() );

              } catch (IllegalArgumentException e) {
                throw new IOException("\"" + data + "\" is not a QName.", e);
              }
              break;
            }
           default:
             throw new IOException(
                 "Cannot write a record of XML Schema Type " + baseType);
          }
          break;
        }
      default:
        throw new IOException("Cannot write data of type " + schema.getType());
      }
    }
  }

  public XmlDatumWriter(XmlDatumConfig config, Schema avroSchema)
      throws IOException {

    if (config == null) {
      throw new IllegalArgumentException("XmlDatumConfig cannot be null.");
    }

    xmlSchemaCollection = new XmlSchemaCollection();
    xmlSchemaCollection.setSchemaResolver(new XmlSchemaMultiBaseUriResolver());
    xmlSchemaCollection.setBaseUri(config.getBaseUri());
    for (StreamSource source : config.getSources()) {
      xmlSchemaCollection.read(source);
    }

    final XmlSchemaStateMachineGenerator stateMachineGen =
        new XmlSchemaStateMachineGenerator();

    final XmlSchemaWalker walker =
        new XmlSchemaWalker(xmlSchemaCollection, stateMachineGen);
    walker.setUserRecognizedTypes( Utils.getAvroRecognizedTypes() );

    AvroSchemaGenerator avroSchemaGen = null;
    if (avroSchema == null) {
      avroSchemaGen =
          new AvroSchemaGenerator(
              config.getBaseUri(),
              config.getSchemaUrls(),
              config.getSchemaFiles());
      walker.addVisitor(avroSchemaGen);
    }

    final XmlSchemaElement rootElement =
        xmlSchemaCollection.getElementByQName(config.getRootTagName());
    walker.walk(rootElement);

    stateMachine = stateMachineGen.getStartNode();

    if (avroSchema == null) {
      schema = avroSchemaGen.getSchema();
    } else {
      schema = avroSchema;
    }
  }

  public XmlDatumWriter(XmlDatumConfig config) throws IOException {
    this(config, null);
  }

  /**
   * Returns the {@link Schema} this <code>XmlDatumWriter</code> is
   * writing against - either the one automatically generated from
   * the {@link XmlDatumConfig} or the {@link Schema} set after that.
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * Sets the schema to use when writing the XML
   * {@link Document} to the {@link Encoder}.
   *
   * @see org.apache.avro.io.DatumWriter#setSchema(org.apache.avro.Schema)
   */
  @Override
  public void setSchema(Schema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Avro schema cannot be null.");
    }
    this.schema = schema;
  }

  /**
   * Writes the {@link Document} to the {@link Encoder} in accordance
   * with the {@link Schema} set in {@link #setSchema(Schema)}.
   *
   * <p>
   * If no {@link Schema} was provided, builds one from the {@link Document}
   * and its {@link XmlSchemaCollection}.  The schema can then be retrieved
   * from {@link #getSchema()}.
   * </p>
   *
   * @see DatumWriter#write(java.lang.Object, org.apache.avro.io.Encoder)
   */
  @Override
  public void write(Document doc, Encoder out) throws IOException {
    // 1. Build the path through the schema that describes the document.
    XmlSchemaPathFinder pathFinder = new XmlSchemaPathFinder(stateMachine);
    SaxWalkerOverDom walker = new SaxWalkerOverDom(pathFinder);
    try {
      walker.walk(doc);
    } catch (Exception se) {
      throw new IOException("Unable to parse the document.", se);
    }
    final XmlSchemaPathNode<AvroRecordInfo, AvroPathNode> path =
        pathFinder.getXmlSchemaDocumentPath();

    // 2. Apply Avro schema metadata on top of the document. 
    final AvroSchemaApplier applier = new AvroSchemaApplier(schema, false);
    applier.apply(path);

    // 3. Encode the document.
    walker.removeContentHandler(pathFinder);
    walker.addContentHandler( new Writer(path, out) );

    try {
      walker.walk(doc);
    } catch (SAXException e) {
      throw new IOException("Unable to encode the document.", e);
    }
  }
}
