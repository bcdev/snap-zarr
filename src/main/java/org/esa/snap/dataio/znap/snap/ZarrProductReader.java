package org.esa.snap.dataio.znap.snap;

import com.bc.ceres.core.ProgressMonitor;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import org.esa.snap.core.dataio.AbstractProductReader;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductReader;
import org.esa.snap.core.dataio.ProductReaderPlugIn;
import org.esa.snap.core.dataio.dimap.DimapProductConstants;
import org.esa.snap.core.dataio.geocoding.ComponentFactory;
import org.esa.snap.core.dataio.geocoding.ComponentGeoCoding;
import org.esa.snap.core.dataio.geocoding.ForwardCoding;
import org.esa.snap.core.dataio.geocoding.GeoChecks;
import org.esa.snap.core.dataio.geocoding.GeoRaster;
import org.esa.snap.core.dataio.geocoding.InverseCoding;
import org.esa.snap.core.dataio.geometry.VectorDataNodeIO;
import org.esa.snap.core.dataio.geometry.VectorDataNodeReader;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ColorPaletteDef;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.DataNode;
import org.esa.snap.core.datamodel.FlagCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.GeometryDescriptor;
import org.esa.snap.core.datamodel.ImageInfo;
import org.esa.snap.core.datamodel.IndexCoding;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.PlacemarkDescriptor;
import org.esa.snap.core.datamodel.PlacemarkDescriptorRegistry;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.datamodel.Stx;
import org.esa.snap.core.datamodel.StxFactory;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.datamodel.VectorDataNode;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.image.ResolutionLevel;
import org.esa.snap.core.util.Debug;
import org.esa.snap.core.util.FeatureUtils;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.core.util.SystemUtils;
import org.geotools.referencing.CRS;
import org.geotools.referencing.factory.ReferencingObjectFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import ucar.ma2.InvalidRangeException;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_FORWARD_CODING_KEY;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_GEO_CHECKS;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_GEO_CRS;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_INVERSE_CODING_KEY;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_LAT_VARIABLE_NAME;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_LON_VARIABLE_NAME;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_OFFSET_X;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_OFFSET_Y;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_RASTER_RESOLUTION_KM;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_SUBSAMPLING_X;
import static org.esa.snap.core.dataio.geocoding.ComponentGeoCodingPersistable.TAG_SUBSAMPLING_Y;
import static org.esa.snap.core.util.Guardian.assertEquals;
import static org.esa.snap.core.util.Guardian.assertNotNull;
import static org.esa.snap.core.util.Guardian.assertNotNullOrEmpty;
import static org.esa.snap.core.util.SystemUtils.LOG;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_MASKS;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_MEANINGS;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_VALUES;
import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.*;
import static ucar.nc2.constants.ACDD.TIME_END;
import static ucar.nc2.constants.ACDD.TIME_START;
import static ucar.nc2.constants.CDM.FILL_VALUE;
import static ucar.nc2.constants.CF.ADD_OFFSET;
import static ucar.nc2.constants.CF.LONG_NAME;
import static ucar.nc2.constants.CF.MISSING_VALUE;
import static ucar.nc2.constants.CF.SCALE_FACTOR;
import static ucar.nc2.constants.CF.UNITS;

public class ZarrProductReader extends AbstractProductReader {

    private ProductReaderPlugIn binaryReaderPlugIn;
    private String binaryFileExtension;
    private HashMap<DataNode, Product> binaryProducts;
    private final Map<ProductNode, Map<String, Object>> nodeAttributes = new HashMap<>();
    private final Map<Map<String, Object>, GeoCoding> sharedGeoCodings = new HashMap<>();

    protected ZarrProductReader(ProductReaderPlugIn readerPlugIn) {
        super(readerPlugIn);
    }

    @Override
    public void readBandRasterData(Band destBand, int destOffsetX, int destOffsetY, int destWidth, int destHeight, ProductData destBuffer, ProgressMonitor pm) {
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
        final Path rootPath = convertToPath(getInput());
        final ZarrGroup rootGroup = ZarrGroup.open(rootPath);
        final Map<String, Object> productAttributes = rootGroup.getAttributes();

        final String productName = (String) productAttributes.get(ATT_NAME_PRODUCT_NAME);
        final String productType = (String) productAttributes.get(ATT_NAME_PRODUCT_TYPE);
        final String productDesc = (String) productAttributes.get(ATT_NAME_PRODUCT_DESC);
        final ProductData.UTC sensingStart = getTime(productAttributes, TIME_START, rootPath); // "time_coverage_start"
        final ProductData.UTC sensingStop = getTime(productAttributes, TIME_END, rootPath); // "time_coverage_end"
        final String product_metadata = (String) productAttributes.get(ATT_NAME_PRODUCT_METADATA);
//        final ArrayList<MetadataElement> metadataElements = toMetadataElements(product_metadata);
        final MetadataElement[] metadataElements = jsonToMetadata(product_metadata);
        final int sceneRasterWidth = ((Number) productAttributes.get(ATT_NAME_PRODUCT_SCENE_WIDTH)).intValue();
        final int sceneRasterHeight = ((Number) productAttributes.get(ATT_NAME_PRODUCT_SCENE_HEIGHT)).intValue();
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
                if (attributes.containsKey(ATT_NAME_BINARY_FORMAT)) {
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
                applyRasterAttributes(attributes, tiePointGrid);
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
                applyBandAttributes(attributes, band);
                if (virtualBand) {
                    continue;
                }
                if (attributes.containsKey(ATT_NAME_BINARY_FORMAT)) {
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
        readVectorData(product);
        return product;
    }

    private void readVectorData(final Product product) {
        final Path rootPath = convertToPath(getInput());
        final Path vectorDataDir = rootPath.resolve(".vector_data");
        if (Files.isDirectory(vectorDataDir)) {
            try {
                final List<Path> paths = Files.list(vectorDataDir).collect(Collectors.toList());
                for (Path path : paths) {
                    addVectorDataToProduct(path, product);
                }
            } catch (IOException e) {
            }
        }
    }

    private void addVectorDataToProduct(Path vectorFilePath, final Product product) {
        final CoordinateReferenceSystem sceneCRS = product.getSceneCRS();
        try (Reader reader = Files.newBufferedReader(vectorFilePath)) {
            FeatureUtils.FeatureCrsProvider crsProvider = new FeatureUtils.FeatureCrsProvider() {
                @Override
                public CoordinateReferenceSystem getFeatureCrs(Product product) {
                    return sceneCRS;
                }

                @Override
                public boolean clipToProductBounds() {
                    return false;
                }
            };
            OptimalPlacemarkDescriptorProvider descriptorProvider = new OptimalPlacemarkDescriptorProvider();
            final String name = vectorFilePath.getFileName().toString();
            VectorDataNode vectorDataNode = VectorDataNodeReader.read(name, reader, product,
                                                                      crsProvider, descriptorProvider, sceneCRS,
                                                                      VectorDataNodeIO.DEFAULT_DELIMITER_CHAR,
                                                                      ProgressMonitor.NULL);
            if (vectorDataNode != null) {
                final ProductNodeGroup<VectorDataNode> vectorDataGroup = product.getVectorDataGroup();
                final VectorDataNode existing = vectorDataGroup.get(vectorDataNode.getName());
                if (existing != null) {
                    vectorDataGroup.remove(existing);
                }
                vectorDataGroup.add(vectorDataNode);
            }
        } catch (IOException e) {
            SystemUtils.LOG.log(Level.SEVERE, "Error reading '" + vectorFilePath + "'", e);
        }
    }

    private void addGeocodings(Map<String, Object> productAttributes, Product product) throws IOException {
        addGeoCoding(product, productAttributes, product::setSceneGeoCoding);
        final List<RasterDataNode> rasterDataNodes = product.getRasterDataNodes();
        for (RasterDataNode rasterDataNode : rasterDataNodes) {
            final Map<String, Object> attibutes = nodeAttributes.get(rasterDataNode);
            if (attibutes != null) {
                addGeoCoding(rasterDataNode, attibutes, rasterDataNode::setGeoCoding);
            }
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
                    final double[] longitudes = lonRaster.getGeophysicalImage().getImage(0).getData()
                            .getPixels(0, 0, rasterWidth, rasterHeight, new double[size]);
                    final double[] latitudes = latRaster.getGeophysicalImage().getImage(0).getData()
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
            ProductData destBuffer, ProgressMonitor pm) {
        throw new IllegalStateException("Data is provided by images");
    }

    static void applyBandAttributes(Map<String, Object> attributes, Band band) {
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
        applySampleCodings(attributes, band);
        applyRasterAttributes(attributes, band);
    }

    static void applyRasterAttributes(Map<String, Object> attributes, RasterDataNode rasterDataNode) {
        if (attributes.get(LONG_NAME) != null) {
            rasterDataNode.setDescription((String) attributes.get(LONG_NAME));
        }
        if (attributes.get(UNITS) != null) {
            rasterDataNode.setUnit((String) attributes.get(UNITS));
        }
        if (attributes.get(SCALE_FACTOR) != null) {
            rasterDataNode.setScalingFactor(((Number) attributes.get(SCALE_FACTOR)).doubleValue());
        }
        if (attributes.get(ADD_OFFSET) != null) {
            rasterDataNode.setScalingOffset(((Number) attributes.get(ADD_OFFSET)).doubleValue());
        }
        if (getNoDataValue(attributes) != null) {
            rasterDataNode.setNoDataValue(getNoDataValue(attributes).doubleValue());
        }
        if (attributes.get(NO_DATA_VALUE_USED) != null) {
            rasterDataNode.setNoDataValueUsed((Boolean) attributes.get(NO_DATA_VALUE_USED));
        }
        if (attributes.get(VALID_PIXEL_EXPRESSION) != null) {
            rasterDataNode.setValidPixelExpression((String) attributes.get(VALID_PIXEL_EXPRESSION));
        }
        if (attributes.get(DimapProductConstants.TAG_IMAGE_TO_MODEL_TRANSFORM) != null) {
            final List matrix = (List) attributes.get(DimapProductConstants.TAG_IMAGE_TO_MODEL_TRANSFORM);
            LOG.info("matrix for band '" + rasterDataNode.getName() + "' = " + Arrays.toString(matrix.toArray()));

            final double[] ma = new double[matrix.size()];
            for (int i = 0; i < matrix.size(); i++) {
                ma[i] = (double) matrix.get(i);
            }
            final AffineTransform i2m = new AffineTransform(ma);
            rasterDataNode.setImageToModelTransform(i2m);
        }
        if (attributes.containsKey(STATISTICS)) {
            final Map<String, Object> stxM = (Map<String, Object>) attributes.get(STATISTICS);
            final double minSample = ((Number) stxM.get(DimapProductConstants.TAG_STX_MIN)).doubleValue();
            final double maxSample = ((Number) stxM.get(DimapProductConstants.TAG_STX_MAX)).doubleValue();
            final double meanSample = ((Number) stxM.get(DimapProductConstants.TAG_STX_MEAN)).doubleValue();
            final double stdDev = ((Number) stxM.get(DimapProductConstants.TAG_STX_STDDEV)).doubleValue();
            final boolean intHistogram = !ProductData.isFloatingPointType(rasterDataNode.getGeophysicalDataType());
            final int[] bins = ((List<Integer>)stxM.get(DimapProductConstants.TAG_HISTOGRAM))
                    .stream().mapToInt(Integer::intValue).toArray();
            final int resLevel = ((Number) stxM.get(DimapProductConstants.TAG_STX_LEVEL)).intValue();
            final Stx stx = new StxFactory()
                    .withMinimum(minSample)
                    .withMaximum(maxSample)
                    .withMean(meanSample)
                    .withStandardDeviation(stdDev)
                    .withIntHistogram(intHistogram)
                    .withHistogramBins(bins == null ? new int[0] : bins)
                    .withResolutionLevel(resLevel).create();
            rasterDataNode.setStx(stx);
        }
        if (attributes.containsKey(IMAGE_INFO)) {
            final Map<String, Object> infoM = (Map<String, Object>) attributes.get(IMAGE_INFO);
            final List<Map<String, Object>> pointsL = (List<Map<String, Object>>) infoM.get(COLOR_PALETTE_POINTS);
            final ArrayList<ColorPaletteDef.Point> points = new ArrayList<>();
            for (Map<String, Object> pointM : pointsL) {
                final ColorPaletteDef.Point point = new ColorPaletteDef.Point();
                if (pointM.containsKey(LABEL)) {
                    String label = (String) pointM.get(LABEL);
                    label = label != null? label.trim(): label;
                    if (StringUtils.isNotNullAndNotEmpty(label)) {
                        point.setLabel(label);
                    }
                }
                point.setSample(((Number) pointM.get(SAMPLE)).doubleValue());
                final int[] rgba = ((List<Integer>) pointM.get(COLOR_RGBA))
                        .stream().mapToInt(Integer::intValue).toArray();
                point.setColor(createColor(rgba));
                points.add(point);
            }

            final ColorPaletteDef colorPaletteDef = new ColorPaletteDef(points.toArray(new ColorPaletteDef.Point[0]));
            colorPaletteDef.setNumColors((int)infoM.get(COLOR_PALETTE_NUM_COLORS));
            colorPaletteDef.setDiscrete((boolean) infoM.get(COLOR_PALETTE_DISCRETE));
            colorPaletteDef.setAutoDistribute((boolean)infoM.get(COLOR_PALETTE_AUTO_DISTRIBUTE));
            final ImageInfo imageInfo = new ImageInfo(colorPaletteDef);
            if (infoM.containsKey(NO_DATA_COLOR_RGBA)) {
                final int[] rgba = ((List<Integer>)infoM.get(NO_DATA_COLOR_RGBA))
                        .stream().mapToInt(Integer::intValue).toArray();
                imageInfo.setNoDataColor(createColor(rgba));
            }
            final String matching = (String) infoM.get(HISTOGRAM_MATCHING);
            final ImageInfo.HistogramMatching histogramMatching = ImageInfo.HistogramMatching.valueOf(matching);
            imageInfo.setHistogramMatching(histogramMatching);
            imageInfo.setLogScaled((boolean) infoM.get(LOG_10_SCALED));
            if(infoM.containsKey(UNCERTAINTY_BAND_NAME)) {
                imageInfo.setUncertaintyBandName((String) infoM.get(UNCERTAINTY_BAND_NAME));
            }
            if(infoM.containsKey(UNCERTAINTY_VISUALISATION_MODE)) {
                final String modeName = (String) infoM.get(UNCERTAINTY_VISUALISATION_MODE);
                final ImageInfo.UncertaintyVisualisationMode mode;
                mode = ImageInfo.UncertaintyVisualisationMode.valueOf(modeName);
                imageInfo.setUncertaintyVisualisationMode(mode);
            }
            rasterDataNode.setImageInfo(imageInfo);
        }
    }

    private static Color createColor(int[] rgba) {
        return new Color(rgba[0], rgba[1], rgba[2], rgba[3]);
    }

    private static void applySampleCodings(Map<String, Object> attributes, Band band) {
        final String rasterName = band.getName();
        final List<String> flagMeanings = (List) attributes.get(FLAG_MEANINGS);
        if (flagMeanings != null) {

            final List<Number> flagMasks = (List<Number>) attributes.get(FLAG_MASKS);
            final List<Double> flagValues = (List<Double>) attributes.get(FLAG_VALUES);

            FlagCoding flagCoding = null;
            IndexCoding indexCoding = null;
            final Product product = band.getProduct();
            String sampleCodingName = getSampleCodingName(attributes, rasterName);
            if (flagMasks != null) {
                flagCoding = new FlagCoding(sampleCodingName);
                band.setSampleCoding(flagCoding);
                product.getFlagCodingGroup().add(flagCoding);
            } else if (flagValues != null) {
                indexCoding = new IndexCoding(sampleCodingName);
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

    private ProductData.UTC getTime(Map<String, Object> productAttributes, String attributeName, Path rootPath) throws IOException {
        if (!productAttributes.containsKey(attributeName)) {
            return null;
        }
        final String iso8601String = (String) productAttributes.get(attributeName);
        try {
            return ISO8601ConverterWithMlliseconds.parse(iso8601String);
        } catch (ParseException e) {
            throw new IOException("Unparseable " + attributeName + " while reading product '" + rootPath.toString() + "'", e);
        }
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

    private static class OptimalPlacemarkDescriptorProvider
            implements VectorDataNodeReader.PlacemarkDescriptorProvider {

        @Override
        public PlacemarkDescriptor getPlacemarkDescriptor(SimpleFeatureType simpleFeatureType) {
            PlacemarkDescriptorRegistry placemarkDescriptorRegistry = PlacemarkDescriptorRegistry.getInstance();
            if (simpleFeatureType.getUserData().containsKey(
                    PlacemarkDescriptorRegistry.PROPERTY_NAME_PLACEMARK_DESCRIPTOR)) {
                String placemarkDescriptorClass = simpleFeatureType.getUserData().get(
                        PlacemarkDescriptorRegistry.PROPERTY_NAME_PLACEMARK_DESCRIPTOR).toString();
                PlacemarkDescriptor placemarkDescriptor = placemarkDescriptorRegistry.getPlacemarkDescriptor(
                        placemarkDescriptorClass);
                if (placemarkDescriptor != null) {
                    return placemarkDescriptor;
                }
            }
            final PlacemarkDescriptor placemarkDescriptor = placemarkDescriptorRegistry.getPlacemarkDescriptor(
                    simpleFeatureType);
            if (placemarkDescriptor != null) {
                return placemarkDescriptor;
            } else {
                return placemarkDescriptorRegistry.getPlacemarkDescriptor(GeometryDescriptor.class);
            }
        }
    }
}
