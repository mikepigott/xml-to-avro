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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;
import javax.xml.bind.ValidationException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaUse;
import org.apache.xerces.util.URI;
import org.apache.xerces.util.URI.MalformedURIException;
import org.xml.sax.Attributes;

/**
 * Methods to confirm that an XML element and
 * its attributes all conform to its XML Schema.
 *
 * @author  Mike Pigott
 */
public final class XmlSchemaElementValidator {

  private static DatatypeFactory datatypeFactory = null;

  private static DatatypeFactory getDatatypeFactory() {
    if (datatypeFactory == null) {
      try {
        datatypeFactory = DatatypeFactory.newInstance();
      } catch (DatatypeConfigurationException e) {
        throw new IllegalStateException("Unable to create the DatatypeFactory for validating XML Schema durations.", e);
      }
    }
    return datatypeFactory;
  }

  private static class QNameNamespaceContext implements NamespaceContext {

    private static String NAMESPACE = "http://ws.apache.org/xmlschema/";
    private static String PREFIX = "pfx";

    QNameNamespaceContext() {
      /* We don't actually care *what* the namespace returns,
       * only that it returns *something*.
       */
      prefixes = new ArrayList<String>(1);
      prefixes.add(PREFIX);
    }

    @Override
    public String getNamespaceURI(String prefix) {
      return NAMESPACE;
    }

    @Override
    public String getPrefix(String namespaceURI) {
      return PREFIX;
    }

    @Override
    public Iterator getPrefixes(String namespaceURI) {
      return prefixes.iterator();
    }

    private List<String> prefixes;
  }

  private static QNameNamespaceContext namespaceContext = null;

  private static NamespaceContext getNamespaceContext() {
    if (namespaceContext == null) {
      namespaceContext = new QNameNamespaceContext();
    }
    return namespaceContext;
  }

  static void validateAttributes(
      XmlSchemaStateMachineNode state,
      Attributes attrs) throws ValidationException {

    if ((state == null)
        || (attrs == null)
        || !state
              .getNodeType()
              .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new ValidationException("Niether state nor attrs can be null, and state must be of an SchemaStateMachineNode.Type.ELEMENT node, not " + ((state == null) ? null : state.getNodeType()));
    }

    final QName elemQName = state.getElement().getQName();

    final List<XmlSchemaStateMachineNode.Attribute> attributes =
        state.getAttributes();

    for (XmlSchemaStateMachineNode.Attribute attribute : attributes) {
      final XmlSchemaAttribute xmlSchemaAttr = attribute.getAttribute();
      final QName attrQName = xmlSchemaAttr.getQName();
      final XmlSchemaUse use = xmlSchemaAttr.getUse();

      String value =
          attrs.getValue(attrQName.getNamespaceURI(), attrQName.getLocalPart());

      if (value == null) {
        // A namespace is not always available.
        value = attrs.getValue("", attrQName.getLocalPart());
      }

      if (value != null) {
        value = value.trim();
      }

      // Confirm the attribute is used correctly.
      switch (use) {
      case OPTIONAL:
        break;
      case PROHIBITED:
        if ((value != null) && !value.isEmpty()) {
          throw new ValidationException("Attribute " + attrQName + " was declared 'prohibited' by " + elemQName + " and cannot have a value.");
        }
        break;
      case REQUIRED:
        if ((value == null) || value.isEmpty()) {
          throw new ValidationException("Attribute " + attrQName + " was declared 'required' by " + elemQName + " and must have a value.");
        }
        break;
      case NONE:
        /* An attribute with no usage is optional, which
         * was already taken care of by XmlSchemaWalker.
         */
      default:
        throw new ValidationException("Attribute " + attrQName + " of element " + elemQName + " has an unrecognized usage of " + use + ".");
      }

      /* If the value is null or empty there is no
       * further validation we can perform here.
       */
      if ((value == null) || value.isEmpty()) {
        continue;
      }

      if ( attribute.getType().equals(XmlSchemaTypeInfo.Type.COMPLEX) ) {
        throw new ValidationException("Attribute " + attrQName + " of element " + elemQName + " cannot have a COMPLEX type.");
      }

      validateType(
          "Attribute " + attrQName + " of " + elemQName,
          value,
          attribute.getType());
    }
  }

  static void validateContent(
      XmlSchemaStateMachineNode state,
      String elementContent) throws ValidationException {
    
  }

  private static void validateType(
      String name,
      String value,
      XmlSchemaTypeInfo typeInfo) throws ValidationException {

    if ((value == null) || value.isEmpty()) {
      throw new ValidationException(name + " cannot have a null or empty value!");
    }

    final HashMap<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>>
      facets = typeInfo.getFacets();

    switch ( typeInfo.getType() ) {
    case ATOMIC:
      validateAtomicType(name, value, typeInfo);
      break;
    case LIST:
      {
        /* A list is a whitespace-separated series of elements.
         * Split the list and perform a type-check on the items.
         */
        final String[] values = value.split(" ");
        for (String item : values) {
          validateType(
              name + " item value \"" + item + "\"",
              item,
              typeInfo.getChildTypes().get(0));
        }
        break;
      }
    case UNION:
      {
        /* We just want to confirm that the value we are given
         * validates against at least one of the types; we do
         * not care which one.
         */
        boolean foundValidType = false;
        for (XmlSchemaTypeInfo unionType : typeInfo.getChildTypes()) {
          try {
            validateType(name, value, unionType);
            foundValidType = true;
            break;
          } catch (ValidationException e) {
            // The type did not validate; try another.
          }
        }
        if (!foundValidType) {
          throw new ValidationException(name + " does not validate against any of its union of types.");
        }
        break;
      }
    case COMPLEX:
      // This only validates if the type is mixed.
      if ( !typeInfo.isMixed() ) {
        throw new ValidationException(name + " has a value of \"" + value + "\" but it represents a non-mixed complex type.");
      }
      break;
    default:
      throw new ValidationException(name + " has an unrecognized type of " + typeInfo.getType());
    }
  }

  private static void validateAtomicType(
      String name,
      String value,
      XmlSchemaTypeInfo typeInfo) throws ValidationException {

    if ( !typeInfo.getType().equals(XmlSchemaTypeInfo.Type.ATOMIC) ) {
      throw new ValidationException(name + " must have a type of ATOMIC, not " + typeInfo.getType());

    } else if ((value == null) || value.isEmpty()) {
      throw new ValidationException(name + " cannot have a null or empty value when validating.");
    }

    switch( typeInfo.getBaseType() ) {
    case ANYTYPE:
    case ANYSIMPLETYPE:
    case ANYURI:
    case STRING:
      // Text plus facets.
      DatatypeConverter.parseString(value);
      break;

    case DURATION:
      try {
        getDatatypeFactory().newDuration(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid duration.", iae);
      }
      break;

    case DATETIME:
      try {
        DatatypeConverter.parseDateTime(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid date-time.", iae);
      }
      break;

    case TIME:
      try {
        DatatypeConverter.parseTime(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid time.", iae);
      }
      break;
      
    case DATE:
      try {
        DatatypeConverter.parseDate(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid date.", iae);
      }
      break;

    case YEARMONTH:
      try {
        getDatatypeFactory().newXMLGregorianCalendar(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid Year-Month.", iae);
      }
      break;

    case YEAR:
      try {
        getDatatypeFactory().newXMLGregorianCalendar(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid year.", iae);
      }
      break;

    case MONTHDAY:
      try {
        getDatatypeFactory().newXMLGregorianCalendar(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid month-day.", iae);
      }
      break;

    case DAY:
      try {
        getDatatypeFactory().newXMLGregorianCalendar(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid day.", iae);
      }
      break;

    case MONTH:
      try {
        getDatatypeFactory().newXMLGregorianCalendar(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid month.", iae);
      }
      break;

      // Dates
    case BOOLEAN:
      try {
        DatatypeConverter.parseBoolean(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid boolean.", iae);
      }
      break;

    case BIN_BASE64:
      try {
        DatatypeConverter.parseBase64Binary(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not valid base-64 binary.", iae);
      }
      break;

    case BIN_HEX:
      try {
        DatatypeConverter.parseHexBinary(value);
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not valid hexadecimal binary.", iae);
      }
      break;

    case FLOAT:
      try {
        DatatypeConverter.parseFloat(value);
      } catch (NumberFormatException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid float.", iae);
      }
      break;

    case DECIMAL:
      try {
        DatatypeConverter.parseDecimal(value);
      } catch (NumberFormatException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid decimal.", iae);
      }
      break;

    case DOUBLE:
      try {
        DatatypeConverter.parseDouble(value);
      } catch (NumberFormatException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid double.", iae);
      }
      break;

    case QNAME:
      try {
        DatatypeConverter.parseQName(value, getNamespaceContext());
      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid .", iae);
      }
      break;

    case NOTATION:
      try {
        /* The value space of NOTATION is the set of QNames
         * of notations declared in the current schema.
         */
        final String[] qNames = value.split(" ");
        for (String qName : qNames) {
          DatatypeConverter.parseQName(qName, getNamespaceContext());
        }

      } catch (IllegalArgumentException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid series of QNames.", iae);
      }
      break;

    default:
      throw new ValidationException(name + " has an unrecognized base value type of " + typeInfo.getBaseType());
    }
  }
}
