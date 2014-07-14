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
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.apache.avro.Schema;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAttribute;
import org.apache.ws.commons.schema.XmlSchemaElement;

/**
 * The scope represents the set of types, attributes, and
 * child groups & elements that the current type derives.
 *
 * @author  Mike Pigott
 */
class XmlSchemaScope {

    /**
     * Initializes a new {@link XmlSchemaScope} with a base
     * {@link XmlSchemaElement}.  The element type and
     * attributes will be traversed, and attribute lists
     * and element children will be retrieved.
     *
     * @param element The base element to build the scope from.
     * @param substitutions The master list of substitution groups to pull from.
     */
    XmlSchemaScope(XmlSchemaElement element, Map<String, XmlSchema> xmlSchemasByNamespace, Map<QName, List<XmlSchemaElement>> substitutions) {
	schemasByNamespace = xmlSchemasByNamespace; 
	substitutes = substitutions;

	xmlToAvroTypeMap = new HashMap<QName, Schema.Type>(); 
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "anyType"),       Schema.Type.STRING);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "boolean"),       Schema.Type.BOOLEAN);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "decimal"),       Schema.Type.DOUBLE);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "double"),        Schema.Type.DOUBLE);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "float"),         Schema.Type.FLOAT);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "base64Binary"),  Schema.Type.BYTES);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "hexBinary"),     Schema.Type.BYTES);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "long"),          Schema.Type.LONG);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "int"),           Schema.Type.INT);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "unsignedInt"),   Schema.Type.LONG);
	xmlToAvroTypeMap.put(new QName(URI_2001_SCHEMA_XSD, "unsignedShort"), Schema.Type.INT);
	xmlToAvroTypeMap.put(QNAME_ID, Schema.Type.STRING);
    }

    XmlSchemaScope(XmlSchemaScope child) {
	this.xmlToAvroTypeMap = child.xmlToAvroTypeMap; // Prevents duplication.
	this.substitutes = child.substitutes;
	this.schemasByNamespace = child.schemasByNamespace;
    }

    private static final String URI_2001_SCHEMA_XSD = "http://www.w3.org/2001/XMLSchema";
    private static final QName QNAME_ID = new QName(URI_2001_SCHEMA_XSD, "ID");

    private Map<QName, Schema.Type> xmlToAvroTypeMap;
    private Map<QName, List<XmlSchemaElement>> substitutes;
    private Map<String, XmlSchema> schemasByNamespace;

    private Map<QName, XmlSchemaAttribute> attributes;
    private Set<XmlSchemaElement> children;
}
