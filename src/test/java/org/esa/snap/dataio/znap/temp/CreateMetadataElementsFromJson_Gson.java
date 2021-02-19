/*
 * $
 *
 * Copyright (C) 2010 by Brockmann Consult (info@brockmann-consult.de)
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation. This program is distributed in the hope it will
 * be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package org.esa.snap.dataio.znap.temp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateMetadataElementsFromJson_Gson {

    public static void main(String[] args) throws IOException {
        final Path path = Paths.get("D:\\temp\\zarr_evaluation\\Products\\MER_RR__2CNPDE20041202_013328_000005582032_00332_14414_7736.znap\\.zattrs");
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            final Gson gson = new GsonBuilder().create();
            final Map fromJson = gson.fromJson(reader, Map.class);
            final List<Map<String, Object>> product_metadata = (List) fromJson.get("product_metadata");
            final ArrayList<MetadataElement> snapElements = toMetadataElements(product_metadata);
            System.out.println("NumElements = " + snapElements.size());
        }
    }

    private static ArrayList<MetadataElement> toMetadataElements(List<Map<String, Object>> product_metadata) {
        final ArrayList<MetadataElement> snapElements = new ArrayList<>();
        for (Map<String, Object> jsonElement : product_metadata) {
            final MetadataElementGson element = toGsonMetadataElement(jsonElement);
            final MetadataElement snapElement = new MetadataElement(element.name);
            snapElement.setDescription(element.description);
            addAttributes(snapElement, element.attributes);
            addElements(snapElement, element.elements);
            snapElements.add(snapElement);
        }
        return snapElements;
    }

    private static void addElements(MetadataElement parentElement, ProductNodeGroupGson<MetadataElementGson> elements) {
        if (elements != null) {
            for (MetadataElementGson node : elements.nodeList.nodes) {
                final MetadataElement childElement = new MetadataElement(node.name);
                childElement.setDescription(node.description);
                addAttributes(childElement, node.attributes);
                addElements(childElement, node.elements);
                parentElement.addElement(childElement);
            }
        }
    }

    private static void addAttributes(MetadataElement snapElement, ProductNodeGroupGson<MetadataAttributeGson> attributes) {
        if (attributes != null) {
            for (MetadataAttributeGson node : attributes.nodeList.nodes) {
                final MetadataAttribute attribute;
                if (node.dataType == ProductData.TYPE_ASCII) {
                    attribute = new MetadataAttribute(node.name, node.dataType);
                } else {
                    attribute = new MetadataAttribute(node.name, node.dataType, node.numElems);
                }
                attribute.setDescription(node.description);
                attribute.setReadOnly(node.readOnly);
                attribute.setSynthetic(node.synthetic);
                attribute.setUnit(node.unit);
                final List<Double> data = (List<Double>) node.data._array;
                if (ProductData.TYPE_ASCII == node.dataType) {
                    if (data.size() > 0) {
                        final byte[] bytes = new byte[data.size()];
                        for (int i = 0; i < data.size(); i++) {
                            Double c = data.get(i);
                            bytes[i] = c.byteValue();
                        }
                        attribute.getData().setElems(bytes);
                    }
                } else {
                    for (int i = 0; i < data.size(); i++) {
                        Double v = data.get(i);
                        attribute.getData().setElemDoubleAt(i, v);
                    }
                }
                snapElement.addAttribute(attribute);
            }
        }
    }

    private static MetadataElementGson toGsonMetadataElement(Map<String, Object> jsonElement) {
        final Gson gson = new GsonBuilder().create();
        final String str = gson.toJson(jsonElement);
        final StringReader reader = new StringReader(str);
        return gson.fromJson(reader, MetadataElementGson.class);
    }

    private static class ProductDataGson {

        protected Object _array;
        protected int _type;
    }

    private static class ProductNodeGson {

        protected String name;
        protected String description;
    }

    private static class MetadataAttributeGson extends ProductNodeGson {

        protected int dataType;
        protected int numElems;
        protected ProductDataGson data;
        protected boolean readOnly;
        protected String unit;
        protected boolean synthetic;
    }

    private static class ProductNodeListGson<T extends ProductNodeGson> {

        protected List<T> nodes;
        protected List<T> removedNodes;
    }

    private static class ProductNodeGroupGson<T extends ProductNodeGson> extends ProductNodeGson {

        protected ProductNodeListGson<T> nodeList;
        protected boolean takingOverNodeOwnership;
    }

    private static class MetadataElementGson extends ProductNodeGson {

        protected ProductNodeGroupGson<MetadataElementGson> elements;
        protected ProductNodeGroupGson<MetadataAttributeGson> attributes;
    }
}
