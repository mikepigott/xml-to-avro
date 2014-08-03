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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.w3c.dom.Document;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Reads an XML {@link Document} and writes it to an {@link Encoder}.
   * <p>
   * Generates an Avro {@link Schema} on the fly from the XML Schema itself. 
   * That {@link Schema can be retrieved by calling {@link #getSchema()}.
   * </p>
   *
 *
 * @author  Mike Pigott
 */
public class XmlDatumWriter implements DatumWriter<Document> {

  private static class Writer extends DefaultHandler {
    Writer(XmlSchemaPathNode path, Encoder out) {
      this.path = path;
      this.out = out;

      stack = new ArrayList<XmlSchemaDocumentNode<AvroRecordInfo>>();
      currLocation = null;
      content = null;
      currAnyElem = null;
    }

    @Override
    public void startDocument() throws SAXException {
      currLocation = path;
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

      final QName elemName = new QName(uri, localName);
      walkToElement(elemName);

      if (!currLocation
            .getDirection()
            .equals(XmlSchemaPathNode.Direction.CHILD)
          && !currLocation
               .getDirection()
               .equals(XmlSchemaPathNode.Direction.SIBLING)) {
        throw new IllegalStateException("We are starting an element, so our path node direction should be to a CHILD or SIBLING, not " + currLocation.getDirection());
      }

      if (currLocation
            .getStateMachineNode()
            .getNodeType()
            .equals(XmlSchemaStateMachineNode.Type.ANY)) {

        // This is an any element; we are not processing it.
        currAnyElem = elemName;
        return;
      }

      final XmlSchemaDocumentNode<AvroRecordInfo> doc =
          currLocation.getDocumentNode();
      final AvroRecordInfo recordInfo = doc.getUserDefinedContent();
      final Schema avroSchema = recordInfo.getAvroSchema();

      try {
        out.startItem();
        if (recordInfo.getUnionIndex() >= 0) {
          out.writeIndex( recordInfo.getUnionIndex() );
        }

        /* The last element in the set of fields is the children.  We want
         * to process the children separately as they require future calls
         * to characters() and/or startElement().
         */
        for (int fieldIndex = 0;
            fieldIndex < avroSchema.getFields().size() - 1;
            ++fieldIndex) {

          final Schema.Field field = avroSchema.getFields().get(fieldIndex);
          if (field.name().equals(elemName.getLocalPart())) {
            // We reached the children field early ... not supposed to happen!
            throw new IllegalStateException("The children field is indexed at " + fieldIndex + " when it was expected to be the last element, or " + (avroSchema.getFields().size() - 1) + ".");
          }

          /* Attributes in XML Schema each have their own namespace, which
           * is not supported in Avro.  So, we will see if we can find the
           * attribute using the existing namespace, and if not, we will
           * walk all of them to see which one has the same name.
           */
          String value =
              atts.getValue(elemName.getNamespaceURI(), field.name());

          if (value == null) {
            for (int attrIndex = 0;
                attrIndex < atts.getLength();
                ++attrIndex) {
              if (atts.getLocalName(attrIndex).equals( field.name() )) {
                value = atts.getValue(attrIndex);
                break;
              }
            }
          }

          try {
            write(field.schema(), value);
          } catch (IOException ioe) {
            throw new SAXException("Could not write " + field.name() + " in " + avroSchema.toString() + " to the output stream for element " + elemName, ioe);
          }
        }

        // If there are children, we want to start an array and end it later.
        if (recordInfo.getNumChildren() > 0) {
          out.writeArrayStart();
          out.setItemCount( recordInfo.getNumChildren() );
        }

        stack.add(currLocation.getDocumentNode());

      } catch (IOException ioe) {
        throw new SAXException("Unable to write " + elemName + " to the output stream.", ioe);
      }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
      if (currAnyElem != null) {
        // We do not process any elements.
        return;
      }

      if (stack.isEmpty()) {
        throw new SAXException("We are processing content, but the element stack is empty!");
      }

      currLocation = currLocation.getNext();
      if ((currLocation == null)
          || !currLocation
                .getDirection()
                .equals(XmlSchemaPathNode.Direction.CONTENT)) {
        throw new SAXException("We are processing characters for " + stack.get(stack.size() - 1).getStateMachineNode().getElement().getQName() + " but the current direction is " + currLocation.getDirection() + ", not CONTENT.");

      } else if (currLocation.getNext() == null) {
        throw new SAXException("We are processing characters for " + stack.get(stack.size() - 1).getStateMachineNode().getElement().getQName() + " but somehow the path ends here!");
      }

      /* If characters() will be called multiple times, we want to collect
       * all of them in the "content" StringBuilder, then process it all
       * once the last bit of content has been collected.
       *
       * If this is the last content node, we'll just write it all out here.
       */
      final boolean moreContentComing =
          currLocation
            .getNext()
            .getDirection()
            .equals(XmlSchemaPathNode.Direction.CONTENT);

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
        final Schema avroSchema =
            stack
              .get(stack.size() - 1)
              .getUserDefinedContent()
              .getAvroSchema();

        try {
          write(avroSchema, result);
        } catch (IOException ioe) {
          final QName elemQName =
              stack
                .get(stack.size() - 1)
                .getStateMachineNode()
                .getElement()
                .getQName();
          throw new SAXException("Unable to write the content \"" + result + "\" for " + elemQName + "", ioe);
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
      final QName elemName = new QName(uri, localName);

      if ((currAnyElem != null) && currAnyElem.equals(elemName)) {
        // We are exiting an any element; prepare for the next one!
        currAnyElem = null;
        return;
      }

      final XmlSchemaDocumentNode<AvroRecordInfo> docNode =
          stack.remove(stack.size() - 1);

      final QName stackElemName =
          docNode
            .getStateMachineNode()
            .getElement()
            .getQName();

      if (!stackElemName.equals(elemName)) {
        throw new SAXException("We are leaving " + elemName + " but the element on the stack is " + stackElemName + ".");
      }

      if (docNode.getUserDefinedContent().getNumChildren() > 0) {
        try {
          out.writeArrayEnd();
        } catch (IOException ioe) {
          throw new SAXException("Unable to end the array for " + elemName, ioe);
        }
      }
    }

    @Override
    public void endDocument() throws SAXException {
      if (currLocation.getNext() != null) {
        throw new IllegalStateException("Reached the end of the document, but the path has more nodes.");
      }
    }

    private void walkToElement(QName elemName) {
      while ((currLocation != null)
          && !currLocation
                .getStateMachineNode()
                .getNodeType()
                .equals(XmlSchemaStateMachineNode.Type.ELEMENT)
          && !currLocation
                .getStateMachineNode()
                .getNodeType()
                .equals(XmlSchemaStateMachineNode.Type.ANY)) {
        currLocation = currLocation.getNext();
      }

      if (currLocation == null) {
        throw new IllegalStateException("Cannot find " + elemName + " in the path!");
      } else if (
          currLocation
            .getStateMachineNode()
            .equals(XmlSchemaStateMachineNode.Type.ELEMENT)
          && !currLocation
                .getStateMachineNode()
                .getElement()
                .getQName()
                .equals(elemName)) {
        throw new IllegalStateException("The next element in the path is " + currLocation.getStateMachineNode().getElement().getQName() + ", not " + elemName + ".");
      }
    }

    private void write(Schema schema, String data)
        throws IOException {

      /* If the data is empty or null, write
       * it as a null or string, if possible.
       */
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
        if (isNullable) {
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
          throw new IOException("Cannot write a null or empty string as a non-null or non-string type.");
        }

        return;
      }

      switch ( schema.getType() ) {
      case UNION:
        {
          Schema textType = null;
          Schema bytesType = null;
          final List<Schema> subTypes = schema.getTypes();
          boolean written = false;
          for (Schema subType : subTypes) {
            // Try the text types last.
            if (subType.getType().equals(Schema.Type.BYTES)) {
              bytesType = subType;
              continue;
            } else if (subType.getType().equals(Schema.Type.STRING)) {
              textType = subType;
              continue;
            }

            try {
              write(subType, data);
              written = true;
              break;
            } catch (IOException ioe) {
              /* Could not parse the value using the
               * provided type; try the next one.
               */
            }
          }

          if (!written) {
            if (bytesType != null) {
              try {
                write(bytesType, data);
                written = true;
              } catch (IOException ioe) {
                // Cannot write the data as bytes either.
              }
            }
            if (!written && (textType != null)) {
              out.writeString(data);

            } else if (!written) {
              throw new IOException("Cannot write \"" + data + "\" as one of the types in " + schema.toString());
            }
          }
          break;
        }
      case BYTES:
        {
          throw new UnsupportedOperationException("Byte types are not supported yet!");
        }
      case STRING:
        {
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
          out.writeEnum( schema.getEnumOrdinal(data) );
          break;
        }
      case DOUBLE:
        {
          try {
            out.writeDouble( Double.parseDouble(data) );
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a double.", nfe);
          }
          break;
        }
      case FLOAT:
        {
          try {
            out.writeFloat( Float.parseFloat(data) );
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a float.", nfe);
          }
          break;
        }
      case LONG:
        {
          try {
            out.writeLong( Long.parseLong(data) );
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not a long.", nfe);
          }
          break;
        }
      case INT:
        {
          try {
            out.writeInt( Integer.parseInt(data) );
          } catch (NumberFormatException nfe) {
            throw new IOException("\"" + data + "\" is not an int.", nfe);
          }
          break;
        }
      case BOOLEAN:
        {
          out.writeBoolean( Boolean.parseBoolean(data) );
          break;
        }
      default:
        throw new IOException("Cannot write data of type " + schema.getType());
      }
    }

    private XmlSchemaPathNode currLocation;
    private StringBuilder content;
    private QName currAnyElem;
    private ArrayList<XmlSchemaDocumentNode<AvroRecordInfo>> stack;

    private final XmlSchemaPathNode path;
    private final Encoder out;
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

    AvroSchemaGenerator avroSchemaGen = null;
    if (avroSchema == null) {
      avroSchemaGen = new AvroSchemaGenerator();
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
   * @see org.apache.avro.io.DatumWriter#write(java.lang.Object, org.apache.avro.io.Encoder)
   */
  @Override
  public void write(Document doc, Encoder out) throws IOException {
    // 1. Build the path through the schema that describes the document.
    XmlSchemaPathCreator pathCreator = new XmlSchemaPathCreator(stateMachine);
    SaxWalkerOverDom walker = new SaxWalkerOverDom(pathCreator);
    try {
      walker.walk(doc);
    } catch (Exception se) {
      throw new IOException("Unable to parse the document.", se);
    }
    final XmlSchemaPathNode path = pathCreator.getXmlSchemaDocumentPath();

    // 2. Apply Avro schema metadata on top of the document. 
    final AvroSchemaApplier applier = new AvroSchemaApplier(schema, false);
    applier.apply(path.getDocumentNode());

    // 3. Encode the document.
    walker.removeContentHandler(pathCreator);
    walker.addContentHandler( new Writer(path, out) );

    try {
      walker.walk(doc);
    } catch (SAXException e) {
      throw new IOException("Unable to encode the document.", e);
    }
  }

  private void encodeElement(
      XmlSchemaDocumentNode<AvroRecordInfo> doc,
      Encoder out)
          throws IOException {
    if (!doc
          .getStateMachineNode()
          .getNodeType()
          .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalStateException("Attempted to encode an element when the node type is " + doc.getStateMachineNode().getNodeType());
    }

    final XmlSchemaElement element = doc.getStateMachineNode().getElement();
    final AvroRecordInfo recordInfo = doc.getUserDefinedContent();

    switch ( recordInfo.getAvroSchema().getType() ) {
    case RECORD:
      {
        out.startItem();
        if (recordInfo.getUnionIndex() >= 0) {
          out.writeIndex( recordInfo.getUnionIndex() );
        }
        for (Schema.Field field : recordInfo.getAvroSchema().getFields()) {
          if (field.name().equals( element.getName() )) {
            // These are the children; they will be processed later.
            continue;
          }
          // TODO: Get contents.
        }
        break;
      }
    case MAP:
      // TODO
    default:
      throw new IllegalStateException("Attempted to process an element with a record type of " + recordInfo.getAvroSchema().getType());
    }
  }

  private final XmlSchemaCollection xmlSchemaCollection;
  private final XmlSchemaStateMachineNode stateMachine;
  private Schema schema;
}
