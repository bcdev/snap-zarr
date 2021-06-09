package org.esa.snap.dataio.znap.snap;

import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class ZarrProductWriterTest_DimensionNameGenerator {

    private ZarrProductWriter.DimensionNameGenerator generator;

    @Before
    public void setUp() throws Exception {
        generator = new ZarrProductWriter.DimensionNameGenerator();
    }

    @Test
    public void testThatReturnValueIsTheSameIfConditionsAreTheSame() {
        final String firstTime_22 = generator.getDimensionNameFor("x", 22);
        final String secondTime_22 = generator.getDimensionNameFor("x", 22);
        assertThat(firstTime_22).isEqualTo("x");
        assertThat(secondTime_22).isSameAs(firstTime_22);

        final String firstTime_44 = generator.getDimensionNameFor("x", 44);
        final String secondTime_44 = generator.getDimensionNameFor("x", 44);
        assertThat(firstTime_44).isEqualTo("x_1");
        assertThat(secondTime_44).isSameAs(firstTime_44);
    }

    @Test
    public void testThatEachDimesionStartsWithoutNumberExtension() {
        assertThat(generator.getDimensionNameFor("a", 22)).isEqualTo("a");
        assertThat(generator.getDimensionNameFor("b", 22)).isEqualTo("b");
        assertThat(generator.getDimensionNameFor("c", 22)).isEqualTo("c");
        assertThat(generator.getDimensionNameFor("d", 22)).isEqualTo("d");
    }

    @Test
    public void testThatEachDimesionWithTheSameNameButDifferentSizeBecomesANumberExtendedName() {
        generator.getDimensionNameFor("a", 22);
        generator.getDimensionNameFor("b", 22);
        generator.getDimensionNameFor("c", 22);
        generator.getDimensionNameFor("d", 22);

        assertThat(generator.getDimensionNameFor("a", 44)).isEqualTo("a_1");
        assertThat(generator.getDimensionNameFor("b", 44)).isEqualTo("b_1");
        assertThat(generator.getDimensionNameFor("c", 44)).isEqualTo("c_1");
        assertThat(generator.getDimensionNameFor("d", 44)).isEqualTo("d_1");
    }
}