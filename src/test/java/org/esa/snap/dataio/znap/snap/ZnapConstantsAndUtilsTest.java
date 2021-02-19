package org.esa.snap.dataio.znap.snap;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.ProductData;
import org.junit.Test;

import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.jsonToMetadata;
import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.metadataToJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class ZnapConstantsAndUtilsTest {

    @Test
    public void metadataTransformation_toJsonStringAndBack() throws JsonProcessingException {
        final MetadataElement nested = new MetadataElement("nested");
        nested.setDescription("nested Desc");
        nested.addAttribute(new MetadataAttribute("doub", ProductData.createInstance(new double[]{24.3}), true));
        nested.addAttribute(new MetadataAttribute("floa", ProductData.createInstance(new float[]{2.4f, 3.5f}), true));
        nested.addAttribute(new MetadataAttribute("long", ProductData.createInstance(new long[]{12345678901L}), true));
        nested.addAttribute(new MetadataAttribute("int", ProductData.createInstance(new int[]{12345678}), false));
        nested.addAttribute(new MetadataAttribute("shor", ProductData.createInstance(new short[]{12345}), true));
        nested.addAttribute(new MetadataAttribute("byte", ProductData.createInstance(new byte[]{123}), false));
        nested.addAttribute(new MetadataAttribute("stri", ProductData.createInstance("String att value"), true));

        final MetadataElement m1 = new MetadataElement("M1");
        m1.setDescription("D1");
        m1.addAttribute(new MetadataAttribute("any", ProductData.createInstance(ProductData.TYPE_UTC, new int[]{244, 233, 6}), true));
        m1.addElement(nested);

        final MetadataElement single = new MetadataElement("single");
        single.setDescription("single desc");
        final MetadataAttribute single_att = new MetadataAttribute("single att", new ProductData.Float(new float[]{-124.5F, 8.9F}), false);
        single_att.setUnit("singleUnit");
        single.addAttribute(single_att);

        final MetadataElement[] elements = {m1, single};

        final String json = metadataToJson(elements);
        final MetadataElement[] readElements = jsonToMetadata(json);

        assertThat(readElements.length, is(elements.length));
        for (int i = 0; i < elements.length; i++) {
            MetadataElement e1 = readElements[i];
            MetadataElement e2 = elements[i];
            equalMetadateElements(e1, e2);
        }
    }

    private void equalMetadateElements(MetadataElement e1, MetadataElement e2) {
        assertThat(e1.getName(), is(equalTo(e2.getName())));
        assertThat(e1.getDescription(), is(equalTo(e2.getDescription())));
        assertThat(e1.getNumElements(), is(equalTo(e2.getNumElements())));
        final MetadataElement[] elements1 = e1.getElements();
        final MetadataElement[] elements2 = e2.getElements();
        for (int i = 0; i < elements2.length; i++) {
            MetadataElement v1 = elements1[i];
            MetadataElement v2 = elements2[i];
            equalMetadateElements(v1, v2);
        }
        assertThat(e1.getNumAttributes(), is(equalTo(e2.getNumAttributes())));
        final MetadataAttribute[] attributes1 = e1.getAttributes();
        final MetadataAttribute[] attributes2 = e2.getAttributes();
        for (int i = 0; i < attributes2.length; i++) {
            MetadataAttribute a1 = attributes1[i];
            MetadataAttribute a2 = attributes2[i];
            assertThat(a1.getName(), is(equalTo(a2.getName())));
            assertThat(a1.getDescription(), is(equalTo(a2.getDescription())));
            assertThat(a1.getUnit(), is(equalTo(a2.getUnit())));
            assertThat(a1.getDataType(), is(equalTo(a2.getDataType())));
            assertThat(a1.getNumDataElems(), is(equalTo(a2.getNumDataElems())));
            assertThat(a1.getData().getElems(), is(equalTo(a2.getData().getElems())));
        }
    }
}