package org.esa.snap.dataio.znap.snap;

import com.bc.ceres.core.ProgressMonitor;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.bc.zarr.ZarrUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.dataio.dimap.DimapProductConstants;
import org.esa.snap.core.dataio.geocoding.*;
import org.esa.snap.core.datamodel.*;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.util.Debug;
import org.geotools.referencing.CRS;
import org.geotools.referencing.factory.ReferencingObjectFactory;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.InvalidRangeException;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.*;
import java.util.List;
import java.util.stream.IntStream;

import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.*;
import static org.esa.snap.core.util.Guardian.*;
import static org.esa.snap.core.util.SystemUtils.LOG;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.*;
import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.*;
import static ucar.nc2.constants.CDM.TIME_END;
import static ucar.nc2.constants.CDM.TIME_START;

public class ZarrProductReader extends AbstractProductReader {

    private ProductReaderPlugIn binaryReaderPlugIn;
    private String binaryFileExtension;
    private HashMap<DataNode, Product> binaryProducts;
    private Map<ProductNode, Map<String, Object>> nodeAttributes = new HashMap<>();
    private Map<Map, GeoCoding> sharedGeoCodings = new HashMap<>();

    protected ZarrProductReader(ProductReaderPlugIn readerPlugIn) {
        super(readerPlugIn);
    }

    @Override
    public void readBandRasterData(Band destBand, int destOffsetX, int destOffsetY, int destWidth, int destHeight, ProductData destBuffer, ProgressMonitor pm) throws IOException {
        throw new IllegalStateException("Data is provided by images");
    }

    @Override
    public void close() throws IOException {
        if (binaryProducts != null) {
            for (Product product : binaryProducts.values()) {
                product.dispose();
            }
            binaryProducts.clear();
            binaryProducts = null;
        }
        super.close();
    }

    @Override
    protected Product readProductNodesImpl() throws IOException {
        // TODO: 23.07.2019 SE -- Frage 1 siehe Trello https://trello.com/c/HMw8CxqL/4-fragen-an-norman
        final Path rootPath = convertToPath(getInput());
        final ZarrGroup rootGroup = ZarrGroup.open(rootPath);
        final Map<String, Object> productAttributes = rootGroup.getAttributes();

        final String productName = (String) productAttributes.get(ATT_NAME_PRODUCT_NAME);
        final String productType = (String) productAttributes.get(ATT_NAME_PRODUCT_TYPE);
        final String productDesc = (String) productAttributes.get(ATT_NAME_PRODUCT_DESC);
        final ProductData.UTC sensingStart = getTime(productAttributes, TIME_START, rootPath); // "time_coverage_start"
        final ProductData.UTC sensingStop = getTime(productAttributes, TIME_END, rootPath); // "time_coverage_end"
        final String product_metadata = (String) productAttributes.get(ATT_NAME_PRODUCT_METADATA);
        final ArrayList<MetadataElement> metadataElements = toMetadataElements(product_metadata);
        final int sceneRasterWidth = ((Double) productAttributes.get(ATT_NAME_PRODUCT_SCENE_WIDTH)).intValue();
        final int sceneRasterHeight = ((Double) productAttributes.get(ATT_NAME_PRODUCT_SCENE_HEIGHT)).intValue();
        final Product product = new Product(productName, productType, sceneRasterWidth, sceneRasterHeight, this);
        product.setDescription(productDesc);
        product.setStartTime(sensingStart);
        product.setEndTime(sensingStop);
        if (productAttributes.get(DATASET_AUTO_GROUPING) != null) {
            product.setAutoGrouping((String) productAttributes.get(DATASET_AUTO_GROUPING));
        }
        if (productAttributes.get(QUICKLOOK_BAND_NAME) != null) {
            product.setQuicklookBandName(((String) productAttributes.get(QUICKLOOK_BAND_NAME)).trim());
        }
        for (MetadataElement metadataElement : metadataElements) {
            product.getMetadataRoot().addElement(metadataElement);
        }


        final Set<String> arrayKeys = rootGroup.getArrayKeys();
        for (String arrayKey : arrayKeys) {
            final ZarrArray zarrArray = rootGroup.openArray(arrayKey);
            final String rasterName = getRasterName(arrayKey);

            final int[] shape = zarrArray.getShape();
            final int[] chunks = zarrArray.getChunks();
            final DataType zarrDataType = zarrArray.getDataType();

            final SnapDataType snapDataType = getSnapDataType(zarrDataType);
            final int width = shape[1];
            final int height = shape[0];

            final Map<String, Object> attributes = zarrArray.getAttributes();

            if (attributes != null && attributes.containsKey(ATT_NAME_OFFSET_X)) {
                final double offsetX = (double) attributes.get(ATT_NAME_OFFSET_X);
                final double offsetY = (double) attributes.get(ATT_NAME_OFFSET_Y);
                final double subSamplingX = (double) attributes.get(ATT_NAME_SUBSAMPLING_X);
                final double subSamplingY = (double) attributes.get(ATT_NAME_SUBSAMPLING_Y);
                final float[] dataBuffer = new float[width * height];
                if (attributes != null && attributes.containsKey(ATT_NAME_BINARY_FORMAT)) {
                    initBinaryReaderPlugin(attributes);
                    final Path srcPath = rootPath.resolve(rasterName).resolve(rasterName + binaryFileExtension);
                    final ProductReader reader = binaryReaderPlugIn.createReaderInstance();
                    final Product binaryProduct = reader.readProductNodes(srcPath.toFile(), null);
                    binaryProduct.setProductReader(reader);
                    final Band dataBand = binaryProduct.getBand("data");
                    dataBand.readPixels(0, 0, width, height, dataBuffer);
                    binaryProduct.dispose();
                } else {
                    try {
                        zarrArray.read(dataBuffer, shape, new int[]{0, 0});
                    } catch (InvalidRangeException e) {
                        throw new IOException("InvalidRangeException while reading tie point raster '" + rasterName + "'", e);
                    }
                }
                final TiePointGrid tiePointGrid = new TiePointGrid(rasterName, width, height, offsetX, offsetY, subSamplingX, subSamplingY, dataBuffer);
                if (attributes.containsKey(DISCONTINUITY)) {
                    tiePointGrid.setDiscontinuity(((Number) attributes.get(DISCONTINUITY)).intValue());
                }
                product.addTiePointGrid(tiePointGrid);
                nodeAttributes.put(tiePointGrid, attributes);
            } else {
                final Band band;
                final boolean virtualBand = attributes != null && attributes.containsKey(VIRTUAL_BAND_EXPRESSION);
                if (virtualBand) {
                    final String expr = (String) attributes.get(VIRTUAL_BAND_EXPRESSION);
                    band = new VirtualBand(rasterName, snapDataType.getValue(), width, height, expr);
                } else {
                    band = new Band(rasterName, snapDataType.getValue(), width, height);
                }
                product.addBand(band);
                nodeAttributes.put(band, attributes);
                apply(attributes, band);
                if (virtualBand) {
                    continue;
                }
                if (attributes != null && attributes.containsKey(ATT_NAME_BINARY_FORMAT)) {
                    initBinaryReaderPlugin(attributes);
                    final Path srcPath = rootPath.resolve(rasterName).resolve(rasterName + binaryFileExtension);
                    final ProductReader reader = binaryReaderPlugIn.createReaderInstance();
                    final Product binaryProduct = reader.readProductNodes(srcPath.toFile(), null);
                    binaryProduct.setProductReader(reader);
                    final Band dataBand = binaryProduct.getBand("data");
                    band.setSourceImage(dataBand.getSourceImage());
                } else {
                    final ZarrOpImage zarrOpImage = new ZarrOpImage(band, shape, chunks, zarrArray, ResolutionLevel.MAXRES);
                    band.setSourceImage(zarrOpImage);
                }
            }
        }
        product.setFileLocation(rootPath.toFile());
        product.setProductReader(this);
        product.setModified(false);
        addGeocodings(productAttributes, product);
        return product;
    }

    private void addGeocodings(Map<String, Object> productAttributes, Product product) throws IOException {
        addGeoCoding(product, productAttributes, product::setSceneGeoCoding);
        final List<RasterDataNode> rasterDataNodes = product.getRasterDataNodes();
        for (RasterDataNode rasterDataNode : rasterDataNodes) {
            final Map<String, Object> attibutes = nodeAttributes.get(rasterDataNode);
            addGeoCoding(rasterDataNode, attibutes, rasterDataNode::setGeoCoding);
        }
    }

    private void addGeoCoding(ProductNode node, Map<String, Object> attributes, GeocodingSetter gcSetter) throws IOException {
        if (!attributes.containsKey(ATT_NAME_GEOCODING)) {
            return;
        }
        final Map gcAttribs = (Map) attributes.get(ATT_NAME_GEOCODING);
        if (gcAttribs == null) {
            return;
        }
        if (!gcAttribs.containsKey(ATT_NAME_GEOCODING_SHARED)) {
            gcSetter.setGeocoding(createGeoCoding(node, gcAttribs));
            return;
        }
        final GeoCoding geoCoding;
        if (sharedGeoCodings.containsKey(gcAttribs)) {
            geoCoding = sharedGeoCodings.get(gcAttribs);
        } else {
            geoCoding = createGeoCoding(node, gcAttribs);
            sharedGeoCodings.put(gcAttribs, geoCoding);
        }
        gcSetter.setGeocoding(geoCoding);
    }

    interface GeocodingSetter {
        void setGeocoding(GeoCoding gc);
    }

    GeoCoding createGeoCoding(ProductNode node, Map gcAttribs) throws IOException {
        final Product product = node.getProduct();
        final String type = (String) gcAttribs.get("type");
        LOG.info("------------------------------------------------------------------------");
        if (ComponentGeoCoding.class.getSimpleName().equals(type)) {
            LOG.info("create " + ComponentGeoCoding.class.getSimpleName() + " for " + node.getName());
            try {
                final String forwardKey = getNotEmptyString(gcAttribs, TAG_FORWARD_CODING_KEY);
                final ForwardCoding forwardCoding = ComponentFactory.getForward(forwardKey);

                final String inverseKey = getNotEmptyString(gcAttribs, TAG_INVERSE_CODING_KEY);
                final InverseCoding inverseCoding = ComponentFactory.getInverse(inverseKey);

                final String geoCheckName = getNotEmptyString(gcAttribs, TAG_GEO_CHECKS);
                final GeoChecks geoChecks = GeoChecks.valueOf(geoCheckName);

                final String geoCrsWKT = getNotEmptyString(gcAttribs, TAG_GEO_CRS);
                final CoordinateReferenceSystem geoCRS;
                try {
                    geoCRS = CRS.parseWKT(geoCrsWKT);
                } catch (FactoryException e) {
                    throw new IllegalArgumentException("Unable to parse WKT for geoCRS", e);
                }

                final String lonVarName = getNotEmptyString(gcAttribs, TAG_LON_VARIABLE_NAME);
                final RasterDataNode lonRaster = product.getRasterDataNode(lonVarName);
                if (lonRaster == null) {
                    throw new IllegalArgumentException("Longitude raster '" + lonVarName + "' expected but was null.");
                }

                final String latVarName = getNotEmptyString(gcAttribs, TAG_LAT_VARIABLE_NAME);
                final RasterDataNode latRaster = product.getRasterDataNode(latVarName);
                if (latRaster == null) {
                    throw new IllegalArgumentException("Latitude raster '" + latVarName + "' expected but was null.");
                }

                final double resolution = (Double) getNotNull(gcAttribs, TAG_RASTER_RESOLUTION_KM);
                final double offsetX = (Double) getNotNull(gcAttribs, TAG_OFFSET_X);
                final double offsetY = (Double) getNotNull(gcAttribs, TAG_OFFSET_Y);
                final double subsamplingX = (Double) getNotNull(gcAttribs, TAG_SUBSAMPLING_X);
                final double subsamplingY = (Double) getNotNull(gcAttribs, TAG_SUBSAMPLING_Y);

                final GeoRaster geoRaster;
                if (lonRaster instanceof TiePointGrid) {
                    final int sceneWidth = product.getSceneRasterWidth();
                    final int sceneHeight = product.getSceneRasterHeight();
                    final TiePointGrid lonTPG = (TiePointGrid) lonRaster;
                    final TiePointGrid latTPG = (TiePointGrid) latRaster;

                    final int gridWidth = lonTPG.getGridWidth();
                    final int gridHeight = lonTPG.getGridHeight();

                    final float[] lons = (float[]) lonTPG.getGridData().getElems();
                    final double[] longitudes = IntStream.range(0, lons.length).mapToDouble(i -> lons[i]).toArray();

                    final float[] lats = (float[]) latTPG.getGridData().getElems();
                    final double[] latitudes = IntStream.range(0, lats.length).mapToDouble(i -> lats[i]).toArray();

                    String text;

                    text = "Longitude tie point grid offset X must be " + offsetX + " but was " + lonTPG.getOffsetX();
                    assertEquals(text, lonTPG.getOffsetX(), offsetX);

                    text = "Longitude tie point grid offset Y must be " + offsetY + " but was " + lonTPG.getOffsetY();
                    assertEquals(text, lonTPG.getOffsetY(), offsetY);

                    text = "Longitude tie point grid subsampling X must be " + subsamplingX + " but was " + lonTPG.getSubSamplingX();
                    assertEquals(text, lonTPG.getSubSamplingX(), subsamplingX);

                    text = "Longitude tie point grid subsampling Y must be " + subsamplingY + " but was " + lonTPG.getSubSamplingY();
                    assertEquals(text, lonTPG.getSubSamplingY(), subsamplingY);

                    geoRaster = new GeoRaster(longitudes, latitudes, lonVarName, latVarName, gridWidth, gridHeight,
                                              sceneWidth, sceneHeight, resolution,
                                              offsetX, offsetY, subsamplingX, subsamplingY);
                } else {
                    final int rasterWidth = lonRaster.getRasterWidth();
                    final int rasterHeight = lonRaster.getRasterHeight();
                    final int size = rasterWidth * rasterHeight;
                    final double[] longitudes = lonRaster.getSourceImage().getImage(0).getData()
                            .getPixels(0, 0, rasterWidth, rasterHeight, new double[size]);
                    final double[] latitudes = latRaster.getSourceImage().getImage(0).getData()
                            .getPixels(0, 0, rasterWidth, rasterHeight, new double[size]);
                    geoRaster = new GeoRaster(longitudes, latitudes, lonVarName, latVarName, rasterWidth, rasterHeight,
                                              resolution);
                }

                final ComponentGeoCoding componentGeoCoding = new ComponentGeoCoding(geoRaster, forwardCoding, inverseCoding, geoChecks, geoCRS);
                componentGeoCoding.initialize();
                return componentGeoCoding;
            } catch (IllegalArgumentException e) {
                LOG.warning(createWarning(e));
            }
        } else if (TiePointGeoCoding.class.getSimpleName().equals(type)) {
            LOG.info("create " + TiePointGeoCoding.class.getSimpleName() + " for " + node.getName());
            final TiePointGrid lat = product.getTiePointGrid((String) gcAttribs.get("latGridName"));
            final TiePointGrid lon = product.getTiePointGrid((String) gcAttribs.get("lonGridName"));
            final String geoCRS_wkt = (String) gcAttribs.get("geoCRS_WKT");
            if (geoCRS_wkt != null) {
                final ReferencingObjectFactory factory = new ReferencingObjectFactory();
                try {
                    final CoordinateReferenceSystem geoCRS = factory.createFromWKT(geoCRS_wkt);
                    return new TiePointGeoCoding(lat, lon, geoCRS);
                } catch (FactoryException e) {
                    throw new IOException("Unable to create scene geocoding.", e);
                }
            } else {
                return new TiePointGeoCoding(lat, lon);
            }
        } else if (CrsGeoCoding.class.getSimpleName().equals(type)) {
            LOG.info("create " + CrsGeoCoding.class.getSimpleName() + " for " + node.getName());
            final int width;
            final int height;
            if (node instanceof RasterDataNode) {
                final RasterDataNode rasterDataNode = (RasterDataNode) node;
                width = rasterDataNode.getRasterWidth();
                height = rasterDataNode.getRasterHeight();
            } else {
                width = product.getSceneRasterWidth();
                height = product.getSceneRasterHeight();
            }
            LOG.info("width = " + width);
            LOG.info("height = " + height);
            try {
                final String wkt = (String) gcAttribs.get(DimapProductConstants.TAG_WKT);
                LOG.info("wkt = " + wkt);
                final CoordinateReferenceSystem crs = CRS.parseWKT(wkt);

                final List matrix = (List) gcAttribs.get(DimapProductConstants.TAG_IMAGE_TO_MODEL_TRANSFORM);
                LOG.info("matrix = " + Arrays.toString(matrix.toArray()));

                final double[] ma = new double[matrix.size()];
                for (int i = 0; i < matrix.size(); i++) {
                    ma[i] = (double) matrix.get(i);
                }
                final AffineTransform i2m = new AffineTransform(ma);
                Rectangle imageBounds = new Rectangle(width, height);
                return new CrsGeoCoding(crs, imageBounds, i2m);
            } catch (FactoryException | TransformException e) {
                Debug.trace(e);
            }
        }
        return null;
    }

    private String createWarning(IllegalArgumentException e) {
        final StringWriter out = new StringWriter();
        final PrintWriter pw = new PrintWriter(out);
        pw.print("Unable to create geo-coding    ");
        pw.println(e.getMessage());
        final StackTraceElement[] stackTrace = e.getStackTrace();
        for (StackTraceElement element : stackTrace) {
            pw.println(" ...... " + element.toString());
        }
        pw.flush();
        pw.close();
        return out.toString();
    }

    private String getNotEmptyString(Map map, String key) {
        final Object o = getNotNull(map, key);
        final String s = ((String) o).trim();
        assertNotNullOrEmpty(key, s);
        return s;
    }

    private Object getNotNull(Map map, String key) {
        final Object o = map.get(key);
        assertNotNull(key, o);
        return o;
    }

    private void initBinaryReaderPlugin(Map<String, Object> attributes) {
        if (binaryReaderPlugIn == null) {
            final String binaryFormat = (String) attributes.get(ATT_NAME_BINARY_FORMAT);
            binaryReaderPlugIn = ProductIO.getProductReader(binaryFormat).getReaderPlugIn();
            binaryFileExtension = binaryReaderPlugIn.getDefaultFileExtensions()[0];
            binaryProducts = new HashMap<>();
        }
    }

    private String getRasterName(String arrayKey) {
        return arrayKey.contains("/") ? arrayKey.substring(arrayKey.lastIndexOf("/") + 1) : arrayKey;
    }

    @Override
    protected void readBandRasterDataImpl(
            int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, int sourceStepX, int sourceStepY,
            Band destBand, int destOffsetX, int destOffsetY, int destWidth, int destHeight,
            ProductData destBuffer, ProgressMonitor pm) throws IOException {
        throw new IllegalStateException("Data is provided by images");
    }

    static void apply(Map<String, Object> attributes, Band band) {
        applySampleCodings(attributes, band);
        if (attributes.get(LONG_NAME) != null) {
            band.setDescription((String) attributes.get(LONG_NAME));
        }
        if (attributes.get(UNITS) != null) {
            band.setUnit((String) attributes.get(UNITS));
        }
        if (attributes.get(SCALE_FACTOR) != null) {
            band.setScalingFactor(((Number) attributes.get(SCALE_FACTOR)).doubleValue());
        }
        if (attributes.get(ADD_OFFSET) != null) {
            band.setScalingOffset(((Number) attributes.get(ADD_OFFSET)).doubleValue());
        }
        if (getNoDataValue(attributes) != null) {
            band.setNoDataValue(getNoDataValue(attributes).doubleValue());
        }
        if (attributes.get(NO_DATA_VALUE_USED) != null) {
            band.setNoDataValueUsed((Boolean) attributes.get(NO_DATA_VALUE_USED));
        }
        if (attributes.get(VALID_PIXEL_EXPRESSION) != null) {
            band.setValidPixelExpression((String) attributes.get(VALID_PIXEL_EXPRESSION));
        }
        // TODO: 21.07.2019 SE -- units for bandwidth, wavelength, solarFlux
        if (attributes.get(BANDWIDTH) != null) {
            band.setSpectralBandwidth(((Number) attributes.get(BANDWIDTH)).floatValue());
        }
        if (attributes.get(WAVELENGTH) != null) {
            band.setSpectralWavelength(((Number) attributes.get(WAVELENGTH)).floatValue());
        }
        if (attributes.get(SOLAR_FLUX) != null) {
            band.setSolarFlux(((Number) attributes.get(SOLAR_FLUX)).floatValue());
        }
        if (attributes.get(SPECTRAL_BAND_INDEX) != null) {
            band.setSpectralBandIndex(((Number) attributes.get(SPECTRAL_BAND_INDEX)).intValue());
        }
    }

    private static void applySampleCodings(Map<String, Object> attributes, Band band) {
        final String rasterName = band.getName();
        final List<String> flagMeanings = (List) attributes.get(FLAG_MEANINGS);
        if (flagMeanings != null) {

            final List<Double> flagMasks = (List<Double>) attributes.get(FLAG_MASKS);
            final List<Double> flagValues = (List<Double>) attributes.get(FLAG_VALUES);

            FlagCoding flagCoding = null;
            IndexCoding indexCoding = null;
            final Product product = band.getProduct();
            if (flagMasks != null) {
                flagCoding = new FlagCoding(getSampleCodingName(attributes, rasterName));
                band.setSampleCoding(flagCoding);
                product.getFlagCodingGroup().add(flagCoding);
            } else if (flagValues != null) {
                indexCoding = new IndexCoding(getSampleCodingName(attributes, rasterName));
                band.setSampleCoding(indexCoding);
                product.getIndexCodingGroup().add(indexCoding);
            } else {
                LOG.warning("Raster attributes for '" + rasterName
                                    + "' contains the attribute '" + FLAG_MEANINGS
                                    + "' but neither an attribute '" + FLAG_MASKS
                                    + "' nor an attribute '" + FLAG_VALUES + "'."
                );
                return;
            }
            for (int i = 0; i < flagMeanings.size(); i++) {
                final String meaningName = flagMeanings.get(i);
                final String description = getFlagDescription(attributes, i);
                if (flagMasks != null) {
                    final int flagMask = flagMasks.get(i).intValue();
                    if (flagValues != null) {
                        flagCoding.addFlag(meaningName, flagMask, flagValues.get(i).intValue(), description);
                    } else {
                        flagCoding.addFlag(meaningName, flagMask, description);
                    }
                } else {
                    indexCoding.addIndex(meaningName, flagValues.get(i).intValue(), description);
                }
            }
        }
    }

    private static String getFlagDescription(Map<String, Object> attributes, int pos) {
        if (attributes.containsKey(FLAG_DESCRIPTIONS)) {
            return (String) ((List) attributes.get(FLAG_DESCRIPTIONS)).get(pos);
        }
        return null;
    }

    private static String getSampleCodingName(Map<String, Object> attributes, String rasterName) {
        final String sampleCodingName;
        if (attributes.containsKey(NAME_SAMPLE_CODING)) {
            sampleCodingName = (String) attributes.get(NAME_SAMPLE_CODING);
        } else {
            sampleCodingName = rasterName;
        }
        return sampleCodingName;
    }

    private static ArrayList<MetadataElement> toMetadataElements(String product_metadata_str) {
        List<Map<String, Object>> product_metadata = ZarrUtils.fromJson(new StringReader(product_metadata_str), List.class);
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
                if (node.dataType == ProductData.TYPE_UINT32 && node.numElems == 3 && "utc".equalsIgnoreCase(node.unit)) {
                    final ProductData pd = attribute.getData();
                    attribute.setData(new ProductData.UTC(pd.getElemIntAt(0), pd.getElemIntAt(1), pd.getElemIntAt(2)));
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

    private ProductData.UTC getTime(Map<String, Object> productAttributes, String attributeName, Path rootPath) throws IOException {
        try {
            return ISO8601ConverterWithMlliseconds.parse((String) productAttributes.get(attributeName));
        } catch (ParseException e) {
            throw new IOException("Unparseable " + attributeName + " while reading product '" + rootPath.toString() + "'", e);
        }
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

    private static Number getNoDataValue(Map<String, Object> attributes) {
        Object attribute = attributes.get(FILL_VALUE);
        if (attribute == null) {
            attribute = attributes.get(MISSING_VALUE);
        }
        if (attribute instanceof String) {
            return Double.valueOf((String) attribute);
        }
        if (attribute != null) {
            return (Number) attribute;
        }
        return null;
    }
}
