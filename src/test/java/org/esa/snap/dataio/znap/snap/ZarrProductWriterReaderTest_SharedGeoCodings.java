package org.esa.snap.dataio.znap.snap;

import com.bc.zarr.ZarrGroup;
import org.esa.snap.core.datamodel.*;
import org.esa.snap.dataio.znap.preferences.ZnapPreferencesConstants;
import org.esa.snap.runtime.Config;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.ATT_NAME_GEOCODING;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ZarrProductWriterReaderTest_SharedGeoCodings {

    private Product product;
    private CrsGeoCoding sceneGeoCoding;
    private CrsGeoCoding sharedGC;
    private CrsGeoCoding single_1;
    private CrsGeoCoding single_2;
    private List<Path> tempDirectories = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        final Preferences snap = Config.instance("snap").load().preferences();
        snap.put(ZnapPreferencesConstants.PROPERTY_NAME_USE_ZIP_ARCHIVE, "false");
        product = new Product("test", "type", 3, 4);
        final Date start = new Date();
        final Date end = new Date(start.getTime() + 4000);
        product.setStartTime(ProductData.UTC.create(start, 123));
        product.setEndTime(ProductData.UTC.create(end, 123));

        final Band b0 = product.addBand("b0", ProductData.TYPE_INT8);
        final Band b1 = product.addBand("b1", ProductData.TYPE_INT8);
        final Band b2 = product.addBand("b2", ProductData.TYPE_INT8);
        final Band b3 = product.addBand("b3", ProductData.TYPE_INT8);
        final Band b4 = product.addBand("b4", ProductData.TYPE_INT8);
        final Band b5 = product.addBand("b5", ProductData.TYPE_INT8);
        final Band b6 = product.addBand("b6", ProductData.TYPE_INT8);
        final Band b7 = product.addBand("b7", ProductData.TYPE_INT8);
        final Band b8 = product.addBand("b8", ProductData.TYPE_INT8);
        final Band b9 = product.addBand("b9", ProductData.TYPE_INT8);

        sceneGeoCoding = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3, 4, 14.0, 15.0, 0.2, 0.1);
        sharedGC = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3, 4, 13.0, 15.0, 0.2, 0.1);
        single_1 = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3, 4, 12.0, 15.0, 0.2, 0.1);
        single_2 = new CrsGeoCoding(DefaultGeographicCRS.WGS84, 3, 4, 11.0, 15.0, 0.2, 0.1);
        product.setSceneGeoCoding(sceneGeoCoding);
        b4.setGeoCoding(sharedGC);
        b5.setGeoCoding(sharedGC);
        b6.setGeoCoding(sharedGC);
        b7.setGeoCoding(single_1);
        b8.setGeoCoding(single_2);

        assertSame(b0.getGeoCoding(), sceneGeoCoding);
        assertSame(b1.getGeoCoding(), sceneGeoCoding);
        assertSame(b2.getGeoCoding(), sceneGeoCoding);
        assertSame(b3.getGeoCoding(), sceneGeoCoding);
        assertSame(b4.getGeoCoding(), sharedGC);
        assertSame(b5.getGeoCoding(), sharedGC);
        assertSame(b6.getGeoCoding(), sharedGC);
        assertSame(b7.getGeoCoding(), single_1);
        assertSame(b8.getGeoCoding(), single_2);
        assertSame(b9.getGeoCoding(), sceneGeoCoding);
    }

    @After
    public void tearDown() {
        for (final Path tempDirectory : tempDirectories) {
            try {
                if (Files.exists(tempDirectory)) {
                    final List<Path> list = Files.walk(tempDirectory).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
                    for (Path path : list) {
                        Files.delete(path);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tempDirectories.clear();
    }

    @Test
    public void testThatSharedGeocodingsAreMarkedInZarrAttributes() throws IOException {
        final ZarrProductWriter zarrProductWriter = new ZarrProductWriter(new ZarrProductWriterPlugIn());
        final Path rootPath = createTempDirectory();
        zarrProductWriter.writeProductNodes(product, rootPath);

        //verification
        // Verify that product root group attributes contains a shared geocoding
        final ZarrGroup group = ZarrGroup.open(rootPath);
        final Map geocoding = (Map) group.getAttributes().get(ATT_NAME_GEOCODING);
        assertThat(geocoding, is(notNullValue()));

        // Verify that if a band has its own geocoding shared with other bands
        // then the geo coding attributes must contain the Key "shared" and the value true.
        final Band[] bands = product.getBands();
        for (Band band : bands) {
            final Map<String, Object> attr = group.openArray(band.getName()).getAttributes();
            final boolean bandOwnsGC = band.getGeoCoding() != sceneGeoCoding;
            if (bandOwnsGC) {
                assertThat(attr.containsKey(ATT_NAME_GEOCODING), is(true));
                final Map gcAttr = (Map) attr.get(ATT_NAME_GEOCODING);
            }
        }
    }

    @Test
    public void writeAndRead() throws IOException {
        final ZarrProductWriter writer = new ZarrProductWriter(new ZarrProductWriterPlugIn());
        final Path rootPath = createTempDirectory();
        writer.writeProductNodes(product, rootPath);

        final ZarrProductReader reader = new ZarrProductReader(new ZarrProductReaderPlugIn());
        final Product product = reader.readProductNodes(rootPath, null);

        assertNotNull(product);
        final GeoCoding sceneGeoCoding = product.getSceneGeoCoding();
        final GeoCoding sharedGeoCoding = product.getBand("b4").getGeoCoding();
        assertNotNull(sceneGeoCoding);
        assertNotNull(sharedGeoCoding);
        assertNotEquals(sharedGeoCoding, sceneGeoCoding);

        assertSame(sceneGeoCoding, product.getBand("b0").getGeoCoding());
        assertSame(sceneGeoCoding, product.getBand("b1").getGeoCoding());
        assertSame(sceneGeoCoding, product.getBand("b2").getGeoCoding());
        assertSame(sceneGeoCoding, product.getBand("b3").getGeoCoding());
        assertSame(sceneGeoCoding, product.getBand("b9").getGeoCoding());

        assertSame(sharedGeoCoding, product.getBand("b5").getGeoCoding());
        assertSame(sharedGeoCoding, product.getBand("b6").getGeoCoding());

        assertNotEquals(sceneGeoCoding, product.getBand("b7").getGeoCoding());
        assertNotEquals(sharedGeoCoding, product.getBand("b7").getGeoCoding());
        assertNotEquals(sceneGeoCoding, product.getBand("b8").getGeoCoding());
        assertNotEquals(sharedGeoCoding, product.getBand("b8").getGeoCoding());

        assertNotEquals(product.getBand("b7").getGeoCoding(), product.getBand("b8").getGeoCoding());
    }

    @Test
    public void testThatTheGeneratedOutputsAreEqual() throws IOException {
        final ZarrProductWriter writer = new ZarrProductWriter(new ZarrProductWriterPlugIn());
        final Path rootPath = createTempDirectory();
        writer.writeProductNodes(product, rootPath);

        final ZarrProductReader reader = new ZarrProductReader(new ZarrProductReaderPlugIn());
        final Product product = reader.readProductNodes(rootPath, null);

        final ZarrProductWriter secondWriter = new ZarrProductWriter(new ZarrProductWriterPlugIn());
        final Path secondRoot = createTempDirectory();
        secondWriter.writeProductNodes(product, secondRoot);

        final List<Path> firstList = Files.walk(rootPath).filter(path -> path.getFileName().toString().equals(".zattrs")).collect(Collectors.toList());
        final List<Path> secondList = Files.walk(secondRoot).filter(path -> path.getFileName().toString().equals(".zattrs")).collect(Collectors.toList());
        assertEquals(firstList.size(), secondList.size());
        for (int i = 0; i < firstList.size(); i++) {
            Path firstPath = firstList.get(i);
            Path secondPath = secondList.get(i);
            final List<String> firstLines = Files.readAllLines(firstPath);
            final List<String> secondLines = Files.readAllLines(secondPath);
            assertEquals(firstLines.size(), secondLines.size());
            for (int j = 0; j < firstLines.size(); j++) {
                String firstLine = firstLines.get(j);
                String secondLine = secondLines.get(j);
                assertEquals(firstLine, secondLine);
            }
        }
    }

    private Path createTempDirectory() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("1111_out_" + getClass().getSimpleName());
        tempDirectories.add(tempDirectory);
        return tempDirectory;
    }
}