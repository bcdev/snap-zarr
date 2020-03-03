package org.esa.snap.dataio.znap.snap;

import org.esa.snap.core.dataio.geocoding.*;
import org.esa.snap.core.dataio.geocoding.forward.PixelForward;
import org.esa.snap.core.dataio.geocoding.forward.TiePointBilinearForward;
import org.esa.snap.core.dataio.geocoding.inverse.PixelQuadTreeInverse;
import org.esa.snap.core.dataio.geocoding.inverse.TiePointInverse;
import org.esa.snap.core.datamodel.*;
import org.esa.snap.core.util.SystemUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

public class ZarrProductReaderTest_createGeocoding_ComponentGeoCoding {

    public static final String geoCRS_WKT =
            "GEOGCS[\"TestWGS8.4(DD)\", DATUM[\"TestWGS8.4\", SPHEROID[\"WGTest\", 637813.70, 29.8257223563]], PRIMEM[\"Greenwich\", 0.0], " +
            "UNIT[\"degree\", 0.017453292519943295], AXIS[\"Geolon\", EAST], AXIS[\"Geolat\", NORTH]]";

    private Product product;
    private List<Path> tempDirectories = new ArrayList<>();
    private ZarrProductWriter productWriter;
    private ZarrProductReader productReader;
    private HashMap gcAttribs;
    private ByteArrayOutputStream logOutput;
    private StreamHandler handler;

    @Before
    public void setUp() throws Exception {
        final boolean containsAngles = true;
        final float[] fDataLon = {
                170, 180, -170,
                168, 178, -172,
                166, 176, -174,
                164, 174, -176
        };
        final float[] fDataLat = {
                50, 49, 48,
                49, 48, 47,
                48, 47, 46,
                47, 46, 45
        };
        final TiePointGrid lon = new TiePointGrid("lon", 3, 4, 0.5, 0.5, 5, 5, fDataLon, containsAngles);
        final TiePointGrid lat = new TiePointGrid("lat", 3, 4, 0.5, 0.5, 5, 5, fDataLat);

        final double[] dDataLon = new double[11 * 16];
        final double[] dDataLat = new double[11 * 16];
        double lonstart = 170;
        double latstart = 50;
        for (int y = 0; y < 16; y++) {
            lonstart -= y * 0.5;
            latstart -= y * 0.15;
            for (int x = 0; x < 11; x++) {
                final int idx = y * 11 + x;
                final double lonVal = lonstart + x * 2;
                dDataLon[idx] = lonVal > 180 ? lonVal - 360 : lonVal;
                dDataLat[idx] = latstart - x * 0.5;
            }
        }

        product = new Product("TestProduct", "type", 11, 16);
        final Date now = new Date();
        product.setStartTime(ProductData.UTC.create(now, 0));
        product.setEndTime(ProductData.UTC.create(new Date(now.getTime() + 4000), 0));
        product.addTiePointGrid(lon);
        product.addTiePointGrid(lat);
        final Band lonBand = product.addBand("Long", ProductData.TYPE_FLOAT64);
        final Band latBand = product.addBand("Lati", ProductData.TYPE_FLOAT64);
        lonBand.setDataElems(dDataLon);
        latBand.setDataElems(dDataLat);

//        final double[] longitudes = IntStream.range(0, fDataLon.length).mapToDouble(i -> fDataLon[i]).toArray();
//        final double[] latitudes = IntStream.range(0, fDataLat.length).mapToDouble(i -> fDataLat[i]).toArray();
//        final GeoRaster geoRaster = new GeoRaster(longitudes, latitudes, lon.getName(), lat.getName(), 3, 4, 11, 16, 253, 0.5, 0.5, 5, 5);
//        final ForwardCoding forwardCoding = ComponentFactory.getForward(TiePointBilinearForward.KEY);
//        final InverseCoding inverseCoding = ComponentFactory.getInverse(TiePointInverse.KEY);
//        final CoordinateReferenceSystem geoCRS = CRS.parseWKT(CRS_WKT);
//        final ComponentGeoCoding sceneGeoCoding = new ComponentGeoCoding(geoRaster, forwardCoding, inverseCoding, GeoChecks.ANTIMERIDIAN, geoCRS);
//        sceneGeoCoding.initialize();
//        product.setSceneGeoCoding(sceneGeoCoding);

        productReader = (ZarrProductReader) new ZarrProductReaderPlugIn().createReaderInstance();

        gcAttribs = new HashMap();
        gcAttribs.put("type", ComponentGeoCoding.class.getSimpleName());
        gcAttribs.put(TAG_FORWARD_CODING_KEY, TiePointBilinearForward.KEY);
        gcAttribs.put(TAG_INVERSE_CODING_KEY, TiePointInverse.KEY);
        gcAttribs.put(TAG_GEO_CHECKS, GeoChecks.ANTIMERIDIAN.name());
        gcAttribs.put(TAG_GEO_CRS, geoCRS_WKT);
        gcAttribs.put(TAG_LON_VARIABLE_NAME, "lon");
        gcAttribs.put(TAG_LAT_VARIABLE_NAME, "lat");
        gcAttribs.put(TAG_RASTER_RESOLUTION_KM, 234.0);
        gcAttribs.put(TAG_OFFSET_X, 0.5);
        gcAttribs.put(TAG_OFFSET_Y, 0.5);
        gcAttribs.put(TAG_SUBSAMPLING_X, 5.0);
        gcAttribs.put(TAG_SUBSAMPLING_Y, 5.0);

        logOutput = new ByteArrayOutputStream();
        handler = new StreamHandler(logOutput, new SimpleFormatter());
        SystemUtils.LOG.addHandler(handler);
    }

    @After
    public void tearDown() {
        SystemUtils.LOG.removeHandler(handler);
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
    public void createGeoCoding__allIsfineWithTiePoint() throws IOException {
        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(getLogOutput().trim(), endsWith("create ComponentGeoCoding for TestProduct"));

        assertThat(geoCoding, is(notNullValue()));
        assertThat(geoCoding, is(instanceOf(ComponentGeoCoding.class)));
        final ComponentGeoCoding componentGeoCoding = (ComponentGeoCoding) geoCoding;
        assertThat(componentGeoCoding.isCrossingMeridianAt180(), is(true));
        assertThat(componentGeoCoding.getForwardCoding().getKey(), is(TiePointBilinearForward.KEY));
        assertThat(componentGeoCoding.getInverseCoding().getKey(), is(TiePointInverse.KEY));
    }

    @Test
    public void createGeoCoding__allIsfineWithBands() throws IOException {
        //preparation
        gcAttribs.put(TAG_FORWARD_CODING_KEY, PixelForward.KEY);
        gcAttribs.put(TAG_INVERSE_CODING_KEY, PixelQuadTreeInverse.KEY);
        gcAttribs.put(TAG_LON_VARIABLE_NAME, "Long");
        gcAttribs.put(TAG_LAT_VARIABLE_NAME, "Lati");

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(getLogOutput().trim(), endsWith("create ComponentGeoCoding for TestProduct"));

        assertThat(geoCoding, is(notNullValue()));
        assertThat(geoCoding, is(instanceOf(ComponentGeoCoding.class)));
        final ComponentGeoCoding componentGeoCoding = (ComponentGeoCoding) geoCoding;
        assertThat(componentGeoCoding.isCrossingMeridianAt180(), is(true));
        assertThat(componentGeoCoding.getForwardCoding().getKey(), is(PixelForward.KEY));
        assertThat(componentGeoCoding.getInverseCoding().getKey(), is(PixelQuadTreeInverse.KEY));
    }

    @Test
    public void createGeoCoding_missingForwardCodingKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_FORWARD_CODING_KEY);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_FORWARD_CODING_KEY + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingInverseCodingKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_INVERSE_CODING_KEY);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_INVERSE_CODING_KEY + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingGeoChecksKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_GEO_CHECKS);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_GEO_CHECKS + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingGeoCrsKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_GEO_CRS);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_GEO_CRS + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_geoCrsValueIsNotParseable() throws IOException {
        //preparation
        gcAttribs.put(TAG_GEO_CRS, "unparseable crs");

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("Unable to parse WKT for geoCRS");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingLonVarNameKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_LON_VARIABLE_NAME);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_LON_VARIABLE_NAME + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingLonVariable() throws IOException {
        //preparation
        product.removeTiePointGrid(product.getTiePointGrid("lon"));

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("Longitude raster 'lon' expected but was null.");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingLatVarNameKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_LAT_VARIABLE_NAME);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_LAT_VARIABLE_NAME + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingLatVariable() throws IOException {
        //preparation
        product.removeTiePointGrid(product.getTiePointGrid("lat"));

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("Latitude raster 'lat' expected but was null.");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingRasterResolutionKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_RASTER_RESOLUTION_KM);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_RASTER_RESOLUTION_KM + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingOffsetXKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_OFFSET_X);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_OFFSET_X + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingOffsetYKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_OFFSET_Y);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_OFFSET_Y + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingSubsamplingXKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_SUBSAMPLING_X);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_SUBSAMPLING_X + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    @Test
    public void createGeoCoding_missingSubsamplingYKey() throws IOException {
        //preparation
        gcAttribs.remove(TAG_SUBSAMPLING_Y);

        //execution
        final GeoCoding geoCoding = productReader.createGeoCoding(product, gcAttribs);

        //verification
        assertThat(geoCoding, is(nullValue()));

        final ArrayList<String> orderedExpectations = new ArrayList<>();
        orderedExpectations.add(productReader.getClass().getName());
        orderedExpectations.add("Unable to create geo-coding");
        orderedExpectations.add("[" + TAG_SUBSAMPLING_Y + "] is null");
        orderedExpectations.add(productReader.getClass().getName());

        assertThat(getLogOutput(), stringContainsInOrder(orderedExpectations));
    }

    public String getLogOutput() {
        handler.close();
        return logOutput.toString();
    }

    private Path createTempDirectory() throws IOException {
        final Path tempDirectory = Files.createTempDirectory("1111_out_" + getClass().getSimpleName());
        tempDirectories.add(tempDirectory);
        return tempDirectory;
    }
}