package org.esa.snap.dataio.znap.snap;

import com.bc.zarr.ZarrGroup;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.datamodel.*;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class ZarrProductWriterReaderTest_persist_TiePointGeoCoding {

    private Product product;
    private List<Path> tempDirectories = new ArrayList<>();
    private ZarrProductWriter productWriter;
    private ZarrProductReader productReader;

    @Before
    public void setUp() throws Exception {
        final boolean containsAngles = true;
        final TiePointGrid lon = new TiePointGrid("lon", 3, 4, 0.5, 0.5, 5, 5, new float[]{
                170, 180, -170,
                168, 178, -172,
                166, 176, -174,
                164, 174, -176,
        }, containsAngles);
        final TiePointGrid lat = new TiePointGrid("lat", 3, 4, 0.5, 0.5, 5, 5, new float[]{
                50, 49, 48,
                49, 48, 47,
                48, 47, 46,
                47, 46, 45,
        });

        product = new Product("test", "type", 10, 15);
        final Date now = new Date();
        product.setStartTime(ProductData.UTC.create(now, 0));
        product.setEndTime(ProductData.UTC.create(new Date(now.getTime() + 4000), 0));
        product.addTiePointGrid(lon);
        product.addTiePointGrid(lat);

        product.setSceneGeoCoding(new TiePointGeoCoding(lat, lon));

        productWriter = (ZarrProductWriter) new ZarrProductWriterPlugIn().createWriterInstance();
        productReader = (ZarrProductReader) new ZarrProductReaderPlugIn().createReaderInstance();
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
    public void writeAndRead() throws IOException {
        final Path tempDirectory = createTempDirectory();
        productWriter.writeProductNodes(product, tempDirectory);
        final Product readIn = productReader.readProductNodes(tempDirectory, null);

        assertNotNull(readIn);
        assertEquals(product.getSceneRasterWidth(), readIn.getSceneRasterWidth());
        assertEquals(product.getSceneRasterHeight(), readIn.getSceneRasterHeight());
        assertNotSame(product.getTiePointGrid("lon"), readIn.getTiePointGrid("lon"));
        assertNotSame(product.getTiePointGrid("lat"), readIn.getTiePointGrid("lat"));
        assertEquals(product.getTiePointGrid("lon").getDiscontinuity(), TiePointGrid.DISCONT_AT_180);
        assertEquals(product.getTiePointGrid("lon").getDiscontinuity(), readIn.getTiePointGrid("lon").getDiscontinuity());
        assertEquals(product.getTiePointGrid("lat").getDiscontinuity(), TiePointGrid.DISCONT_NONE);
        assertEquals(product.getTiePointGrid("lat").getDiscontinuity(), readIn.getTiePointGrid("lat").getDiscontinuity());

        final float[] srcLons = (float[]) product.getTiePointGrid("lon").getGridData().getElems();
        final float[] readLons = (float[]) readIn.getTiePointGrid("lon").getGridData().getElems();
        final float[] srcLats = (float[]) product.getTiePointGrid("lat").getGridData().getElems();
        final float[] readLats = (float[]) readIn.getTiePointGrid("lat").getGridData().getElems();

        assertArrayEquals(srcLons, readLons, Float.MIN_VALUE);
        assertArrayEquals(srcLats, readLats, Float.MIN_VALUE);
        assertNotSame(product.getSceneGeoCoding(), readIn.getSceneGeoCoding());
        assertEquals(readIn.getSceneGeoCoding() instanceof TiePointGeoCoding, true);
        final TiePointGeoCoding srcGC = (TiePointGeoCoding) product.getSceneGeoCoding();
        final TiePointGeoCoding readGC = (TiePointGeoCoding) readIn.getSceneGeoCoding();
        assertEquals(srcGC.getLonGrid().getName(), readGC.getLonGrid().getName());
        assertEquals(srcGC.getLatGrid().getName(), readGC.getLatGrid().getName());
        assertEquals(srcGC.isCrossingMeridianAt180(), readGC.isCrossingMeridianAt180());
        assertEquals(srcGC.getNumApproximations(), readGC.getNumApproximations());
        final List<RasterDataNode> rasterDataNodes = readIn.getRasterDataNodes();
        for (int i = 0, rasterDataNodesSize = rasterDataNodes.size(); i < rasterDataNodesSize; i++) {
            RasterDataNode rdn = rasterDataNodes.get(i);
            final String name = rdn.getName();
            final String message = "RasterDataNode name: " + name + " at index " + i;
            assertSame(message, readGC, rdn.getGeoCoding());
        }
    }

    private Path createTempDirectory() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("1111_out_" + getClass().getSimpleName());
        tempDirectories.add(tempDirectory);
        return tempDirectory;
    }
}