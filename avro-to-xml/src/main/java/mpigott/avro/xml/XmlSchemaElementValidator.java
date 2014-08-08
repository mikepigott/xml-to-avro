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

import java.math.BigDecimal;
import java.math.BigInteger;
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

import jregex.Matcher;
import jregex.Pattern;
import jregex.PatternSyntaxException;
import jregex.REFlags;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;
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

    if ((state == null)
        || !state
              .getNodeType()
              .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new ValidationException("Niether state nor attrs can be null, and state must be of an SchemaStateMachineNode.Type.ELEMENT node, not " + ((state == null) ? null : state.getNodeType()));
    }

    final QName elemQName = state.getElement().getQName();
    final XmlSchemaTypeInfo elemType = state.getElementType();
    final XmlSchemaElement element = state.getElement();

    if (elementContent != null) {
      elementContent = elementContent.trim();
    }

    switch ( elemType.getType() ) {
    case COMPLEX:
      {
        if (!elemType.isMixed()
          && (elementContent != null)
          && !elementContent.isEmpty()) {
          
          /* If a type is COMPLEX, then it either is a mixed type or it only
           * has elements as children.  Likewise, if the text is not null or
           * empty, and the type is not mixed, then element content is where
           * it is not expected.
           */
          throw new ValidationException(elemQName + " is a non-mixed complex type, therefore there should not be any content between the tags, like \"" + elementContent + "\".");
        }
        break;
      }
    case ATOMIC:
    case LIST:
    case UNION:
      {
        if ((elementContent == null) || elementContent.isEmpty()) {
          if ( state.getElement().isNillable() ) {
            // Null is a perfectly valid state.
            return;
          } else {
            elementContent = element.getDefaultValue();
            if (elementContent == null) {
              elementContent = element.getFixedValue();
            }
            if (elementContent == null) {
              throw new ValidationException("Element " + elemQName + " has no content, no default value, and no fixed value, but is of type " + elemType.getType() + ".");
            }
          }
        }
        validateType(elemQName.toString(), elementContent, elemType);
        break;
      }
    default:
      throw new IllegalStateException(elemQName + " has an unrecognized content type of " + elemType.getType() + ".");
    }
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
        listLengthChecks(name, values, facets);
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

    final Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets =
        typeInfo.getFacets();

    switch( typeInfo.getBaseType() ) {
    case ANYTYPE:
    case ANYSIMPLETYPE:
    case ANYURI:
      /* anyURI has no equivalent type in Java.
       * (from http://docs.oracle.com/cd/E19159-01/819-3669/bnazf/index.html)
       */
    case STRING:
      // Text plus facets.
      stringLengthChecks(name, DatatypeConverter.parseString(value), facets);
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
        rangeChecks(
            name,
            new BigDecimal( DatatypeConverter.parseFloat(value) ),
            facets);
      } catch (NumberFormatException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid float.", iae);
      }
      break;

    case DECIMAL:
      try {
        rangeChecks(
            name,
            DatatypeConverter.parseDecimal(value),
            facets);
      } catch (NumberFormatException iae) {
        throw new ValidationException(name + " value of \"" + value + "\" is not a valid decimal.", iae);
      }
      digitsFacetChecks(name, value, facets);
      break;

    case DOUBLE:
      try {
        rangeChecks(
            name,
            new BigDecimal( DatatypeConverter.parseDouble(value) ),
            facets);
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

    checkEnumerationFacet(name, value, facets);
  }

  private static void rangeChecks(
      String name,
      BigDecimal value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets)
  throws ValidationException {

    if (facets == null) {
      return;
    }

    rangeCheck(name, value, facets, XmlSchemaRestriction.Type.EXCLUSIVE_MIN);
    rangeCheck(name, value, facets, XmlSchemaRestriction.Type.INCLUSIVE_MIN);
    rangeCheck(name, value, facets, XmlSchemaRestriction.Type.EXCLUSIVE_MAX);
    rangeCheck(name, value, facets, XmlSchemaRestriction.Type.INCLUSIVE_MAX);
  }

  private static void rangeCheck(
      String name,
      BigDecimal value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets,
      XmlSchemaRestriction.Type rangeType)
  throws ValidationException {

    final List<XmlSchemaRestriction> rangeFacets = facets.get(rangeType);

    boolean satisfied = true;
    BigDecimal compareTo = null;

    if ((rangeFacets != null) && !rangeFacets.isEmpty()) {
      for (XmlSchemaRestriction rangeFacet : rangeFacets) {
        compareTo = getBigDecimalOf( rangeFacet.getValue() );
        final int comparison = value.compareTo(compareTo);

        switch (rangeType) {
        case EXCLUSIVE_MIN:
          satisfied = (comparison > 0);
          break;
        case INCLUSIVE_MIN:
          satisfied = (comparison >= 0);
          break;
        case EXCLUSIVE_MAX:
          satisfied = (comparison < 0);
          break;
        case INCLUSIVE_MAX:
          satisfied = (comparison <= 0);
          break;
        default:
          throw new ValidationException("Cannot perform a range check of type " + rangeType);
        }

        if (!satisfied) {
          break;
        }
      }
    }

    if (!satisfied) {
      throw new ValidationException(name + " value \"" + value + "\" violates the " + rangeType + " restriction of " + compareTo + ".");
    }
  }

  private static BigDecimal getBigDecimalOf(Object numericValue) {
    BigDecimal newValue = null;

    if (numericValue instanceof BigDecimal) {
      newValue = (BigDecimal) numericValue;

    } else if (numericValue instanceof Double) {
      newValue = new BigDecimal(((Double) numericValue).doubleValue());

    } else if (numericValue instanceof Float) {
      newValue = new BigDecimal(((Float) numericValue).floatValue());

    } else if (numericValue instanceof BigInteger) {
      newValue = new BigDecimal((BigInteger) numericValue);

    } else if (numericValue instanceof Number) {
      newValue = new BigDecimal(((Number) numericValue).longValue());

    } else {
      throw new IllegalArgumentException(numericValue.getClass().getName() + " is not a subclass of java.lang.Number.");
    }

    return newValue;
  }

  private static void stringLengthChecks(
      String name,
      String value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets)
  throws ValidationException {

    if (facets == null) {
      return;
    }

    stringLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH);
    stringLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH_MIN);
    stringLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH_MAX);
  }

  private static void stringLengthCheck(
      String name,
      String value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets,
      XmlSchemaRestriction.Type facetType)
  throws ValidationException {

    final List<XmlSchemaRestriction> lengthFacets = facets.get(facetType);
    int lengthRestriction = -1;
    boolean satisfied = true;

    if (lengthFacets != null) {
      for (XmlSchemaRestriction lengthFacet : lengthFacets) {
        lengthRestriction = Integer.parseInt( lengthFacet.getValue().toString() );
        switch (facetType) {
        case LENGTH:
          satisfied = (value.length() == lengthRestriction);
          break;
        case LENGTH_MIN:
          satisfied = (value.length() >= lengthRestriction);
          break;
        case LENGTH_MAX:
          satisfied = (value.length() <= lengthRestriction);
          break;
        default:
          throw new IllegalArgumentException("Cannot perform a length restriction of type " + facetType);
        }

        if (!satisfied) {
          break;
        }
      }
    }

    if (!satisfied) {
      throw new ValidationException(name + " value \"" + value + "\" does not meet the " + facetType + " restriction of " + lengthRestriction + ".");
    }
  }

  private static void listLengthChecks(
      String name,
      String[] value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets)
  throws ValidationException {

    if (facets == null) {
      return;
    }

    listLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH);
    listLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH_MIN);
    listLengthCheck(name, value, facets, XmlSchemaRestriction.Type.LENGTH_MAX);
  }

  private static void listLengthCheck(
      String name,
      String[] value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets,
      XmlSchemaRestriction.Type facetType)
  throws ValidationException {

    final List<XmlSchemaRestriction> lengthFacets = facets.get(facetType);
    int lengthRestriction = -1;
    boolean satisfied = true;

    if (lengthFacets != null) {
      for (XmlSchemaRestriction lengthFacet : lengthFacets) {
        lengthRestriction = Integer.parseInt( lengthFacet.getValue().toString() );
        switch (facetType) {
        case LENGTH:
          satisfied = (value.length == lengthRestriction);
          break;
        case LENGTH_MIN:
          satisfied = (value.length >= lengthRestriction);
          break;
        case LENGTH_MAX:
          satisfied = (value.length <= lengthRestriction);
          break;
        default:
          throw new IllegalArgumentException("Cannot perform a length restriction of type " + facetType);
        }

        if (!satisfied) {
          break;
        }
      }
    }

    if (!satisfied) {
      throw new ValidationException(name + " value of length " + value.length + " does not meet the " + facetType + " restriction of " + lengthRestriction + ".");
    }
  }

  private static void digitsFacetChecks(
      String name,
      String value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets)
  throws ValidationException {

    if (facets == null) {
      return;
    }

    String[] numSplit = value.split("\\.");
    if ((numSplit.length == 0) || (numSplit.length > 2)) {
      throw new ValidationException(name + " value \"" + value + "\" is expected to have one or two sections around the decimal point, not " + numSplit.length + ".");
    }

    if (numSplit.length > 1) {
      digitsFacetCheck(
          name,
          numSplit,
          facets,
          XmlSchemaRestriction.Type.DIGITS_FRACTION);
    }

    digitsFacetCheck(
        name,
        numSplit,
        facets,
        XmlSchemaRestriction.Type.DIGITS_TOTAL);
  }

  private static void digitsFacetCheck(
      String name,
      String[] value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets,
      XmlSchemaRestriction.Type facetType)
  throws ValidationException {

    final List<XmlSchemaRestriction> digitsFacets = facets.get(facetType);
    int numDigits = 0;
    boolean satisfied = true;

    if (digitsFacets != null) {
      for (XmlSchemaRestriction digitsFacet : digitsFacets) {
        numDigits = Integer.parseInt( digitsFacet.getValue().toString() );
        switch (facetType) {
        case DIGITS_FRACTION:
          satisfied = (value[1].length() <= numDigits);
          break;
        case DIGITS_TOTAL:
        {
          int totalDigits = value[0].length();
          if (value.length == 2) {
            totalDigits += value[1].length();
          }
          satisfied = (totalDigits <= numDigits);
          break;
        }
        default:
          throw new IllegalArgumentException("Cannot perform a digits facet check with a facet of type " + facetType);
        }
      }
    }

    if (!satisfied) {
      StringBuilder errMsg = new StringBuilder(name);
      errMsg.append(" value \"").append(value[0]);
      if (value.length > 1) {
        errMsg.append('.').append(value[1]);
      }
      errMsg.append(" does not meet the ").append(facetType);
      errMsg.append(" check of ").append(numDigits).append(" digits.");

      throw new ValidationException( errMsg.toString() );
    }
  }

  private static void checkEnumerationFacet(
      String name,
      String value,
      Map<XmlSchemaRestriction.Type, List<XmlSchemaRestriction>> facets)
  throws ValidationException {

    if (facets == null) {
      return;
    }

    final List<XmlSchemaRestriction> enumFacets =
        facets.get(XmlSchemaRestriction.Type.ENUMERATION);

    if (enumFacets == null) {
      return;
    }

    boolean found = false;
    for (XmlSchemaRestriction enumFacet : enumFacets) {
      if ( value.equals(enumFacet.getValue().toString()) ) {
        found = true;
        break;
      }
    }

    if (!found) {
      StringBuilder errMsg = new StringBuilder(name);
      errMsg.append(" value \"").append(value).append("\" is not a member of");
      errMsg.append(" the enumeration {\"");
      for (int enumIndex = 0; enumIndex < enumFacets.size() - 1; ++enumIndex) {
        errMsg.append( enumFacets.get(enumIndex).getValue() ).append("\", \"");
      }
      errMsg.append( enumFacets.get(enumFacets.size() - 1).getValue() );
      errMsg.append("\"}.");

      throw new ValidationException( errMsg.toString() );
    }
  }
}