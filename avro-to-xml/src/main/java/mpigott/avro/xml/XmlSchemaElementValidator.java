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

import java.util.HashMap;
import java.util.List;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaUse;
import org.xml.sax.Attributes;

/**
 * Methods to confirm that an XML element and
 * its attributes all conform to its XML Schema.
 *
 * @author  Mike Pigott
 */
public final class XmlSchemaElementValidator {

  static void validateAttributes(
      XmlSchemaStateMachineNode state,
      Attributes attrs) {

    if ((state == null)
        || (attrs == null)
        || !state
              .getNodeType()
              .equals(XmlSchemaStateMachineNode.Type.ELEMENT)) {
      throw new IllegalArgumentException("Niether state nor attrs can be null, and state must be of an SchemaStateMachineNode.Type.ELEMENT node, not " + ((state == null) ? null : state.getNodeType()));
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
          throw new IllegalArgumentException("Attribute " + attrQName + " was declared 'prohibited' by " + elemQName + " and cannot have a value.");
        }
        break;
      case REQUIRED:
        if ((value == null) || value.isEmpty()) {
          throw new IllegalArgumentException("Attribute " + attrQName + " was declared 'required' by " + elemQName + " and must have a value.");
        }
        break;
      case NONE:
        /* An attribute with no usage is optional, which
         * was already taken care of by XmlSchemaWalker.
         */
      default:
        throw new IllegalArgumentException("Attribute " + attrQName + " of element " + elemQName + " has an unrecognized usage of " + use + ".");
      }

      /* If the value is null or empty there is no
       * further validation we can perform here.
       */
      if ((value == null) || value.isEmpty()) {
        continue;
      }

      if ( attribute.getType().equals(XmlSchemaTypeInfo.Type.COMPLEX) ) {
        throw new IllegalArgumentException("Attribute " + attrQName + " of element " + elemQName + " cannot have a COMPLEX type.");
      }

      validateType(
          "Attribute " + attrQName + " of " + elemQName,
          value,
          attribute.getType());
    }
  }

  static void validateContent(
      XmlSchemaStateMachineNode state,
      String elementContent) {
    
  }

  private static void validateType(
      String name,
      String value,
      XmlSchemaTypeInfo typeInfo) {

    if ((value == null) || value.isEmpty()) {
      throw new IllegalArgumentException(name + " cannot have a null or empty value!");
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
          } catch (Exception e) {
            // The type did not validate; try another.
          }
        }
        if (!foundValidType) {
          throw new IllegalArgumentException(name + " does not validate against any of its union of types.");
        }
        break;
      }
    case COMPLEX:
      // This only validates if the type is mixed.
      if ( !typeInfo.isMixed() ) {
        throw new IllegalArgumentException(name + " has a value of \"" + value + "\" but it represents a non-mixed complex type.");
      }
      break;
    default:
      throw new IllegalArgumentException(name + " has an unrecognized type of " + typeInfo.getType());
    }
  }

  private static void validateAtomicType(
      String name,
      String value,
      XmlSchemaTypeInfo typeInfo) {

    if ( !typeInfo.getType().equals(XmlSchemaTypeInfo.Type.ATOMIC) ) {
      throw new IllegalArgumentException(name + " must have a type of ATOMIC, not " + typeInfo.getType());
    }

    switch( typeInfo.getBaseType() ) {
      
    }
  }
}
