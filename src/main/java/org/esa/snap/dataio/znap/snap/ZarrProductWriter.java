package org.esa.snap.dataio.znap.snap;

import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.glevel.MultiLevelImage;
import com.bc.zarr.ArrayParams;
import com.bc.zarr.Compressor;
import com.bc.zarr.CompressorFactory;
import com.bc.zarr.DataType;
import com.bc.zarr.ZarrArray;
import com.bc.zarr.ZarrGroup;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.dataio.dimap.DimapProductConstants;
import org.esa.snap.core.dataio.geocoding.ComponentGeoCoding;
import org.esa.snap.core.dataio.geocoding.GeoRaster;
import org.esa.snap.core.dataio.geometry.VectorDataNodeIO;
import org.esa.snap.core.dataio.geometry.WriterBasedVectorDataNodeWriter;
import org.esa.snap.core.dataio.persistence.Item;
import org.esa.snap.core.dataio.persistence.JsonLanguageSupport;
import org.esa.snap.core.dataio.persistence.Persistence;
import org.esa.snap.core.dataio.persistence.PersistenceEncoder;
import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.ColorPaletteDef;
import org.esa.snap.core.datamodel.CrsGeoCoding;
import org.esa.snap.core.datamodel.GeoCoding;
import org.esa.snap.core.datamodel.ImageInfo;
import org.esa.snap.core.datamodel.Mask;
import org.esa.snap.core.datamodel.MetadataAttribute;
import org.esa.snap.core.datamodel.MetadataElement;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;
import org.esa.snap.core.datamodel.ProductNode;
import org.esa.snap.core.datamodel.ProductNodeGroup;
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.core.datamodel.SampleCoding;
import org.esa.snap.core.datamodel.Stx;
import org.esa.snap.core.datamodel.TiePointGeoCoding;
import org.esa.snap.core.datamodel.TiePointGrid;
import org.esa.snap.core.datamodel.VectorDataNode;
import org.esa.snap.core.datamodel.VirtualBand;
import org.esa.snap.core.util.StringUtils;
import org.esa.snap.core.util.SystemUtils;
import org.esa.snap.dataio.znap.preferences.ZnapPreferencesConstants;
import org.esa.snap.runtime.Config;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import ucar.ma2.InvalidRangeException;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.prefs.Preferences;
import java.util.stream.Collectors;

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
import static org.esa.snap.core.util.StringUtils.isNotNullAndNotEmpty;
import static org.esa.snap.core.util.SystemUtils.LOG;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_MASKS;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_MEANINGS;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.FLAG_VALUES;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.tryFindUnitString;
import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.*;
import static ucar.nc2.constants.ACDD.TIME_END;
import static ucar.nc2.constants.ACDD.TIME_START;
import static ucar.nc2.constants.CDM.FILL_VALUE;
import static ucar.nc2.constants.CDM.UNSIGNED;
import static ucar.nc2.constants.CF.ADD_OFFSET;
import static ucar.nc2.constants.CF.LONG_NAME;
import static ucar.nc2.constants.CF.SCALE_FACTOR;
import static ucar.nc2.constants.CF.UNITS;

public class ZarrProductWriter extends AbstractProductWriter {

    public static final String DEFAULT_COMPRESSOR_ID = "zlib";
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    private final HashMap<Band, BinaryWriter> zarrWriters = new HashMap<>();
    private final Compressor compressor;
    private final ProductWriterPlugIn binaryWriterPlugIn;

    private final Persistence persistence = new Persistence();
    private final JsonLanguageSupport languageSupport = new JsonLanguageSupport();

    private ZarrGroup zarrGroup;
    private Path outputRoot;
    private List<GeoCoding> sharedGeoCodings;

    public ZarrProductWriter(final ZarrProductWriterPlugIn productWriterPlugIn) {
        super(productWriterPlugIn);
        compressor = CompressorFactory.create(getCompressorId(), "level", getCompressionLevel());
        binaryWriterPlugIn = getBinaryWriterPlugin(productWriterPlugIn);
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {
        final BinaryWriter binaryWriter = zarrWriters.get(sourceBand);
        final int[] to = {sourceOffsetY, sourceOffsetX}; // common data model manner { y, x }
        final int[] shape = {sourceHeight, sourceWidth};  // common data model manner { y, x }
        try {
            binaryWriter.write(ensureNotLogarithmicData(sourceBand, sourceBuffer), shape, to);
        } catch (InvalidRangeException e) {
            throw new IOException("Invalid range while writing raster '" + sourceBand.getName() + "'", e);
        }
        pm.done();
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        for (BinaryWriter value : zarrWriters.values()) {
            value.dispose();
        }
        zarrWriters.clear();
    }

    @Override
    public void deleteOutput() {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        outputRoot = convertToPath(getOutput());
        final Product product = getSourceProduct();
        sharedGeoCodings = collectSharedGeoCodings(product);
        zarrGroup = ZarrGroup.create(outputRoot, collectProductAttributes());
        writeVectorData();
        for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
            writeTiePointGrid(tiePointGrid);
        }
        for (Band band : product.getBands()) {
            initializeZarrArrayForBand(band);
            if (shouldWrite(band)) {
                initializeZarrBandWriter(band);
            }
        }
    }

    private void writeVectorData() throws IOException {
        Product product = getSourceProduct();
        ProductNodeGroup<VectorDataNode> vectorDataGroup = product.getVectorDataGroup();
        final Path vectorDataDir = outputRoot.resolve(".vector_data");
        if (Files.isDirectory(vectorDataDir)) {
            final List<Path> collect = Files.list(vectorDataDir).collect(Collectors.toList());
            for (Path path : collect) {
                Files.delete(path);
            }
        }

        if (vectorDataGroup.getNodeCount() > 0) {
            Files.createDirectories(vectorDataDir);
            for (int i = 0; i < vectorDataGroup.getNodeCount(); i++) {
                VectorDataNode vectorDataNode = vectorDataGroup.get(i);
                writeVectorData(vectorDataDir, vectorDataNode);
            }
        }
    }

    void writeVectorData(Path vectorDataDir, VectorDataNode vectorDataNode) {
        try {
            WriterBasedVectorDataNodeWriter vectorDataNodeWriter = new WriterBasedVectorDataNodeWriter();
            final Path filePath = vectorDataDir.resolve(vectorDataNode.getName() + VectorDataNodeIO.FILENAME_EXTENSION);
            try (final BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                vectorDataNodeWriter.write(vectorDataNode, writer);
            }
        } catch (IOException e) {
            SystemUtils.LOG.throwing("DimapProductWriter", "writeVectorData", e);
        }
    }

    @Override
    public boolean shouldWrite(ProductNode node) {
        if (node instanceof VirtualBand) {
            return false;
        }
        return super.shouldWrite(node);
    }

    static List<GeoCoding> collectSharedGeoCodings(Product product) {
        final Map<GeoCoding, Integer> counts = new HashMap<>();
        final GeoCoding sceneGeoCoding = product.getSceneGeoCoding();
        if (sceneGeoCoding != null) {
            counts.put(sceneGeoCoding, 1);
        }
        final List<RasterDataNode> rasterDataNodes = product.getRasterDataNodes();
        for (RasterDataNode rasterDataNode : rasterDataNodes) {
            final GeoCoding geoCoding = rasterDataNode.getGeoCoding();
            if (geoCoding != null) {
                if (counts.containsKey(geoCoding)) {
                    int count = counts.get(geoCoding);
                    counts.put(geoCoding, ++count);
                } else {
                    counts.put(geoCoding, 1);
                }
            }
        }

        final ArrayList<GeoCoding> sharedGeoCodings = new ArrayList<>();
        for (GeoCoding geoCoding : counts.keySet()) {
            int shared = counts.get(geoCoding);
            if (shared > 1) {
                sharedGeoCodings.add(geoCoding);
            }
        }

        return sharedGeoCodings;
    }

    Map<String, Object> collectProductAttributes() {
        final Map<String, Object> attributes = new HashMap<>();
        collectGeneralProductAttrs(attributes);
        collectProductGeoCodingAttrs(attributes);
        // flag attributes collected per Band (Flag or Index Band). see collectSampleCodingAttributes()
        collectMaskAttrs(attributes);
        return attributes;
    }

    private void collectMaskAttrs(Map<String, Object> attributes) {
        final ProductNodeGroup<Mask> maskGroup = getSourceProduct().getMaskGroup();
    }

    private void collectProductGeoCodingAttrs(Map<String, Object> attrs) {
        final GeoCoding gc = getSourceProduct().getSceneGeoCoding();
        if (gc != null) {
            attrs.put(ATT_NAME_GEOCODING, getGeoCodingAttributes(gc));
        }
    }

    private void collectGeneralProductAttrs(Map<String, Object> attrs) {
        final Product product = getSourceProduct();
        attrs.put(ATT_NAME_PRODUCT_NAME, product.getName());
        attrs.put(ATT_NAME_PRODUCT_TYPE, product.getProductType());
        attrs.put(ATT_NAME_PRODUCT_DESC, product.getDescription());
        attrs.put(ATT_NAME_PRODUCT_SCENE_WIDTH, product.getSceneRasterWidth());
        attrs.put(ATT_NAME_PRODUCT_SCENE_HEIGHT, product.getSceneRasterHeight());
        addTimeAttribute(attrs, TIME_START, product.getStartTime()); // "time_coverage_start"
        addTimeAttribute(attrs, TIME_END, product.getEndTime()); // "time_coverage_end"

        try {
            final MetadataElement[] metadataElements = product.getMetadataRoot().getElements();
            final String metadataNotPrettyPrint = metadataToJson(metadataElements);
            attrs.put(ATT_NAME_PRODUCT_METADATA, metadataNotPrettyPrint);
        } catch (JsonProcessingException e) {
            // TODO: 17.02.2021 SE -- Turn this into correct exception handing.
            e.printStackTrace();
        }

        if (product.getAutoGrouping() != null) {
            attrs.put(DATASET_AUTO_GROUPING, product.getAutoGrouping().toString());
        }
        if (isNotNullAndNotEmpty(product.getQuicklookBandName())) {
            attrs.put(QUICKLOOK_BAND_NAME, product.getQuicklookBandName());
        }
    }

    Map<String, Object> getGeoCodingAttributes(GeoCoding gc) {
        final HashMap<String, Object> map = new HashMap<>();
        map.put(ATT_NAME_GEOCODING_TYPE, gc.getClass().getSimpleName());
        if (sharedGeoCodings.contains(gc)) {
            map.put(ATT_NAME_GEOCODING_SHARED, true);
        }
        final PersistenceEncoder<Object> encoder = persistence.getEncoder(gc);
        if (encoder != null) {
            final Item item = encoder.encode(gc);
            final Map<String, Object> translation = languageSupport.translateToLanguageObject(item);
            map.put("persistence", translation);
            return map;
        }
        /*if (gc instanceof ComponentGeoCoding) {

            final ComponentGeoCoding componentGC = (ComponentGeoCoding) gc;

            map.put(TAG_FORWARD_CODING_KEY, componentGC.getForwardCoding().getKey());
            map.put(TAG_INVERSE_CODING_KEY, componentGC.getInverseCoding().getKey());
            map.put(TAG_GEO_CHECKS, componentGC.getGeoChecks().name());
            map.put(TAG_GEO_CRS, componentGC.getGeoCRS().toWKT().replace("\n", "").replace("\r", ""));

            final GeoRaster geoRaster = componentGC.getGeoRaster();

            map.put(TAG_LON_VARIABLE_NAME, geoRaster.getLonVariableName());
            map.put(TAG_LAT_VARIABLE_NAME, geoRaster.getLatVariableName());
            map.put(TAG_RASTER_RESOLUTION_KM, geoRaster.getRasterResolutionInKm());
            map.put(TAG_OFFSET_X, geoRaster.getOffsetX());
            map.put(TAG_OFFSET_Y, geoRaster.getOffsetY());
            map.put(TAG_SUBSAMPLING_X, geoRaster.getSubsamplingX());
            map.put(TAG_SUBSAMPLING_Y, geoRaster.getSubsamplingY());
        } else*/
        if (gc instanceof TiePointGeoCoding) {
            final TiePointGeoCoding tpgc = (TiePointGeoCoding) gc;
            final String latGridName = tpgc.getLatGrid().getName();
            map.put("latGridName", latGridName);
            final String lonGridName = tpgc.getLonGrid().getName();
            map.put("lonGridName", lonGridName);
            final CoordinateReferenceSystem geoCRS = tpgc.getGeoCRS();
            if (geoCRS != null) {
                map.put("geoCRS_WKT", geoCRS.toWKT());
            }
        } else if (gc instanceof CrsGeoCoding) {
            final CrsGeoCoding crsGeoCoding = (CrsGeoCoding) gc;
            final String wkt = crsGeoCoding.getMapCRS().toString().replaceAll("\n *", "").replaceAll("\r *", "");
            map.put(DimapProductConstants.TAG_WKT, wkt);
            final double[] matrix = new double[6];
            ((AffineTransform) crsGeoCoding.getImageToMapTransform()).getMatrix(matrix);
            map.put(DimapProductConstants.TAG_IMAGE_TO_MODEL_TRANSFORM, matrix);
        }
        return map;
    }

    static void collectRasterAttributes(RasterDataNode rdNode, Map<String, Object> attributes) {
        if (rdNode.getDescription() != null) {
            attributes.put(LONG_NAME, rdNode.getDescription());
        }
        if (rdNode.getUnit() != null) {
            attributes.put(UNITS, tryFindUnitString(rdNode.getUnit()));
        }
        final int nodeDataType = rdNode.getDataType();
        if (ProductData.isUIntType(nodeDataType)) {
            attributes.put(UNSIGNED, String.valueOf(true));
        }
        collectNoDataValue(rdNode, attributes);

        if (isNotNullAndNotEmpty(rdNode.getValidPixelExpression())) {
            attributes.put(VALID_PIXEL_EXPRESSION, rdNode.getValidPixelExpression());
        }
        AffineTransform imageToModelTransform = rdNode.getImageToModelTransform();
        if (!imageToModelTransform.isIdentity()) {
            final double[] matrix = new double[6];
            imageToModelTransform.getMatrix(matrix);
            attributes.put(DimapProductConstants.TAG_IMAGE_TO_MODEL_TRANSFORM, matrix);

        }
        if (rdNode.isStxSet()) {
            final LinkedHashMap<String, Object> stxM = new LinkedHashMap<>();
            attributes.put(STATISTICS, stxM);
            final Stx stx = rdNode.getStx();
            stxM.put(DimapProductConstants.TAG_STX_MIN, stx.getMinimum());
            stxM.put(DimapProductConstants.TAG_STX_MAX, stx.getMaximum());
            stxM.put(DimapProductConstants.TAG_STX_MEAN, stx.getMean());
            stxM.put(DimapProductConstants.TAG_STX_STDDEV, stx.getStandardDeviation());
            stxM.put(DimapProductConstants.TAG_STX_LEVEL, stx.getResolutionLevel());
            final int[] bins = stx.getHistogramBins();
            if (bins != null && bins.length > 0) {
                stxM.put(DimapProductConstants.TAG_HISTOGRAM, bins);
            }
        }

        final ImageInfo imageInfo = rdNode.getImageInfo();
        if (imageInfo != null) {
            final LinkedHashMap<String, Object> infoM = new LinkedHashMap<>();

            final ColorPaletteDef paletteDef = imageInfo.getColorPaletteDef();
            final ArrayList<Map> pointsL = new ArrayList<>();
            for (ColorPaletteDef.Point point : paletteDef.getPoints()) {
                final LinkedHashMap<String, Object> pointM = new LinkedHashMap<>();
                if (StringUtils.isNotNullAndNotEmpty(point.getLabel())) {
                    pointM.put(LABEL, point.getLabel());
                }
                pointM.put(SAMPLE, point.getSample());
                pointM.put(COLOR_RGBA, createColorObject(point.getColor()));
                pointsL.add(pointM);
            }
            infoM.put(COLOR_PALETTE_POINTS, pointsL);
            infoM.put(COLOR_PALETTE_NUM_COLORS, paletteDef.getNumColors());
            infoM.put(COLOR_PALETTE_AUTO_DISTRIBUTE, paletteDef.isAutoDistribute());
            infoM.put(COLOR_PALETTE_DISCRETE, paletteDef.isDiscrete());
            final Color noDataColor = imageInfo.getNoDataColor();
            if (noDataColor != null) {
                infoM.put(NO_DATA_COLOR_RGBA, createColorObject(noDataColor));
            }
            infoM.put(HISTOGRAM_MATCHING, imageInfo.getHistogramMatching().toString());
            infoM.put(LOG_10_SCALED, imageInfo.isLogScaled());
            if (StringUtils.isNotNullAndNotEmpty(imageInfo.getUncertaintyBandName())) {
                infoM.put(UNCERTAINTY_BAND_NAME, imageInfo.getUncertaintyBandName());
            }
            if (imageInfo.getUncertaintyVisualisationMode() != null) {
                final ImageInfo.UncertaintyVisualisationMode mode = imageInfo.getUncertaintyVisualisationMode();
                infoM.put(UNCERTAINTY_VISUALISATION_MODE, mode.toString());
            }
            attributes.put(IMAGE_INFO, infoM);
        }
    }

    private static Object createColorObject(Color c) {
        return new int[]{c.getRed(), c.getGreen(), c.getBlue(), c.getAlpha()};
    }

    static void collectBandAttributes(Band band, Map<String, Object> attributes) {
        if (band.getSpectralBandwidth() > 0) {
            attributes.put(BANDWIDTH, band.getSpectralBandwidth());
            attributes.put(BANDWIDTH + UNIT_EXTENSION, BANDWIDTH_UNIT);
        }
        if (band.getSpectralWavelength() > 0) {
            attributes.put(WAVELENGTH, band.getSpectralWavelength());
            attributes.put(WAVELENGTH + UNIT_EXTENSION, WAVELENGTH_UNIT);
        }
        if (band.getSolarFlux() > 0) {
            // TODO: 15.11.2019 SE -- unit for solarFlux
            attributes.put(SOLAR_FLUX, band.getSolarFlux());
        }
        if ((float) band.getSpectralBandIndex() >= 0) {
            attributes.put(SPECTRAL_BAND_INDEX, band.getSpectralBandIndex());
        }
    }

    private void addTimeAttribute(Map<String, Object> productAttributes, String attName, ProductData.UTC utc) {
        if (utc != null) {
            productAttributes.put(attName, ISO8601ConverterWithMlliseconds.format(utc));
        }
    }

    private ProductData ensureNotLogarithmicData(Band sourceBand, ProductData data) {
        final boolean log10Scaled = sourceBand.isLog10Scaled();
        if (!log10Scaled) {
            return data;
        }
        final ProductData scaledData = ProductData.createInstance(ProductData.TYPE_FLOAT32, data.getNumElems());
        for (int i = 0; i < data.getNumElems(); i++) {
            scaledData.setElemDoubleAt(i, sourceBand.scale(data.getElemDoubleAt(i)));
        }
        return scaledData;
    }

    static void collectSampleCodingAttributes(Band band, Map<String, Object> attributes) {
        final SampleCoding sampleCoding = band.getSampleCoding();
        if (sampleCoding == null) {
            return;
        }

        attributes.put(ZnapConstantsAndUtils.NAME_SAMPLE_CODING, sampleCoding.getName());

        final boolean indexBand = band.isIndexBand();
        final boolean flagBand = band.isFlagBand();
        if (!(indexBand || flagBand)) {
            LOG.warning("Band references a SampleCoding but this is neither an IndexCoding nor a FlagCoding.");
            return;
        }

        final int numCodings = sampleCoding.getNumAttributes();
        final String[] names = new String[numCodings];
        final String[] descriptions = new String[numCodings];
        final int[] masks = new int[numCodings];
        final int[] values = new int[numCodings];
        final MetadataAttribute[] codingAtts = sampleCoding.getAttributes();
        boolean alsoFlagValues = false;
        for (int i = 0; i < codingAtts.length; i++) {
            MetadataAttribute attribute = codingAtts[i];
            names[i] = attribute.getName();
            descriptions[i] = attribute.getDescription();
            final ProductData data = attribute.getData();
            if (indexBand) {
                values[i] = data.getElemInt();
            } else {
                masks[i] = data.getElemInt();
                final boolean twoElements = data.getNumElems() == 2;
                values[i] = twoElements ? data.getElemIntAt(1) : data.getElemInt();
                alsoFlagValues = alsoFlagValues || twoElements;
            }
        }
        attributes.put(FLAG_MEANINGS, names);
        if (indexBand) {
            attributes.put(FLAG_VALUES, values);
        } else {
            attributes.put(FLAG_MASKS, masks);
            if (alsoFlagValues) {
                attributes.put(FLAG_VALUES, values);
            }
        }
        if (containsNotEmptyStrings(descriptions, true)) {
            attributes.put(FLAG_DESCRIPTIONS, descriptions);
        }
    }

    private static boolean containsNotEmptyStrings(final String[] strings, final boolean trim) {
        if (strings != null || strings.length > 0) {
            if (trim) {
                for (int i = 0; i < strings.length; i++) {
                    final String string = strings[i];
                    strings[i] = string != null ? string.trim() : string;
                }
            }
            for (final String string : strings) {
                if (string != null && !string.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static Number getZarrFillValue(RasterDataNode node) {
        final Double geophysicalNoDataValue = node.getGeophysicalNoDataValue();
        final DataType zarrDataType = getZarrDataType(node);
        if (zarrDataType == DataType.f8) {
            return geophysicalNoDataValue;
        }
        switch (zarrDataType) {
            case f4:
                return geophysicalNoDataValue.floatValue();
            case i1:
            case u1:
            case i2:
            case u2:
            case i4:
            case u4:
                return geophysicalNoDataValue.longValue();
            default:
                throw new IllegalStateException();
        }
    }

    private static DataType getZarrDataType(RasterDataNode node) {
        if (node.isLog10Scaled()) {
            return com.bc.zarr.DataType.f4;
        }
        final int dataType = node.getDataType();
        switch (dataType) {
            case ProductData.TYPE_FLOAT64:
                return DataType.f8;
            case ProductData.TYPE_FLOAT32:
                return DataType.f4;
            case ProductData.TYPE_INT8:
                return DataType.i1;
            case ProductData.TYPE_INT16:
                return DataType.i2;
            case ProductData.TYPE_INT32:
                return DataType.i4;
            case ProductData.TYPE_UINT8:
                return DataType.u1;
            case ProductData.TYPE_UINT16:
                return DataType.u2;
            case ProductData.TYPE_UINT32:
                return DataType.u4;
            default:
                throw new IllegalStateException();
        }
    }

    private String getCompressorId() {
        String compressorID = getPreference(ZnapPreferencesConstants.PROPERTY_NAME_COMPRESSOR_ID, DEFAULT_COMPRESSOR_ID);
        compressorID = compressorID != null ? compressorID.trim() : compressorID;
        if (!DEFAULT_COMPRESSOR_ID.equals(compressorID)) {
            LOG.info("Znap format product writer will use '" + compressorID + "' compression.");
        }
        return compressorID;
    }

    private int getCompressionLevel() {
        final String value = getPreference(ZnapPreferencesConstants.PROPERTY_NAME_COMPRESSION_LEVEL, "" + DEFAULT_COMPRESSION_LEVEL);
        final int compressionLevel = Integer.parseInt(value);
        if (compressionLevel != DEFAULT_COMPRESSION_LEVEL) {
            LOG.info("Znap format product writer will use " + compressionLevel + " compression level.");
        }
        return compressionLevel;
    }

    private ProductWriterPlugIn getBinaryWriterPlugin(ZarrProductWriterPlugIn productWriterPlugIn) {
        String defaultBinaryFormatName = productWriterPlugIn.getFormatNames()[0];
        final String binaryFormatName = getPreference(ZnapPreferencesConstants.PROPERTY_NAME_BINARY_FORMAT, defaultBinaryFormatName);
        if (defaultBinaryFormatName.equals(binaryFormatName)) {
            return null;
        }
        LOG.info("Binary data in Znap format should be written as '" + binaryFormatName + "'");
        final ProductWriterPlugIn writerPlugIn = ProductIO.getProductWriter(binaryFormatName).getWriterPlugIn();
        if (writerPlugIn == null) {
            throw new IllegalArgumentException("Unable to write binary data as '" + binaryFormatName + "'.");
        }
        return writerPlugIn;
    }

    private String getPreference(String key, String _default) {
        return getPreferences().get(key, _default);
    }

    private Preferences getPreferences() {
        return Config.instance("snap").load().preferences();
    }

    private void writeTiePointGrid(TiePointGrid tiePointGrid) throws IOException {
        final int[] shape = {tiePointGrid.getGridHeight(), tiePointGrid.getGridWidth()}; // common data model manner { y, x }
        final String name = tiePointGrid.getName();
        final Map<String, Object> attributes = collectTiePointGridAttributes(tiePointGrid);
        final ArrayParams arrayParams = new ArrayParams()
                .dataType(getZarrDataType(tiePointGrid))
                .shape(shape)
                .fillValue(getZarrFillValue(tiePointGrid))
                .compressor(compressor);
        final ZarrArray zarrArray = zarrGroup.createArray(name, arrayParams, attributes);

        final ProductData gridData;
        if (tiePointGrid.getGridData() != null) {
            gridData = tiePointGrid.getData();
        } else {
            gridData = readTiePointGridData(tiePointGrid);
        }
        if (binaryWriterPlugIn == null) {
            try {
                zarrArray.write(gridData.getElems(), shape, new int[]{0, 0});
            } catch (InvalidRangeException e) {
                throw new IOException("Invalid range while writing raster '" + name + "'", e);
            }
        } else {
            final ProductWriter writer = createBinaryProductWriter();
            final Product binaryProduct = new Product("_" + name + "_", "binary", shape[IDX_WIDTH], shape[IDX_HEIGHT]);
            final Band binaryBand = binaryProduct.addBand("data", getSnapDataType(zarrArray.getDataType()).getValue());
            final String fileExtension = binaryWriterPlugIn.getDefaultFileExtensions()[0];
            final Path path = outputRoot.resolve(name).resolve(name + fileExtension);
            writer.writeProductNodes(binaryProduct, path.toFile());
            binaryProduct.setProductWriter(writer);
            binaryBand.writeRasterData(0, 0, shape[IDX_WIDTH], shape[IDX_HEIGHT], gridData);
            binaryProduct.dispose();
        }
    }

    private ProductWriter createBinaryProductWriter() {
        final ProductWriter writer = binaryWriterPlugIn.createWriterInstance();
        switchToIntermediateMode(writer);
        return writer;
    }

    private Map<String, Object> collectTiePointGridAttributes(TiePointGrid tiePointGrid) {
        final Map<String, Object> attributes = new HashMap<>();
        collectRasterAttributes(tiePointGrid, attributes);
        attributes.put(ATT_NAME_OFFSET_X, tiePointGrid.getOffsetX());
        attributes.put(ATT_NAME_OFFSET_Y, tiePointGrid.getOffsetY());
        attributes.put(ATT_NAME_SUBSAMPLING_X, tiePointGrid.getSubSamplingX());
        attributes.put(ATT_NAME_SUBSAMPLING_Y, tiePointGrid.getSubSamplingY());
        if (binaryWriterPlugIn != null) {
            attributes.put(ATT_NAME_BINARY_FORMAT, binaryWriterPlugIn.getFormatNames()[0]);
        }
        final int discontinuity = tiePointGrid.getDiscontinuity();
        if (discontinuity != TiePointGrid.DISCONT_NONE) {
            attributes.put(DISCONTINUITY, discontinuity);
        }
        return attributes;
    }

    private void trimChunks(int[] chunks, int[] shape) {
        for (int i = 0; i < shape.length; i++) {
            if (shape[i] < chunks[i]) {
                chunks[i] = shape[i];
            }
        }
    }

    private void initializeZarrArrayForBand(Band band) throws IOException {
        final int[] shape = getShape(band); // common data model manner { y, x }
        final String name = band.getName();
        final ArrayParams arrayParams = new ArrayParams()
                .dataType(getZarrDataType(band))
                .shape(shape)
                .fillValue(getZarrFillValue(band))
                .compressor(compressor);
        if (binaryWriterPlugIn == null) {
            int[] chunks;
            MultiLevelImage sourceImage = band.getSourceImage();
            chunks = new int[]{sourceImage.getTileHeight(), sourceImage.getTileWidth()}; // common data model manner { y, x }
            trimChunks(chunks, shape);
            arrayParams.chunks(chunks);
        } else {
            arrayParams.chunked(false);
        }
        zarrGroup.createArray(name, arrayParams, collectBandAttributes(band));
    }

    private void initializeZarrBandWriter(Band band) throws IOException {
        int[] shape = getShape(band);
        String name = band.getName();
        ZarrArray zarrArray = zarrGroup.openArray(name);
        final BinaryWriter binaryWriter;
        if (binaryWriterPlugIn == null) {
            binaryWriter = new StandardZarrChunksWriter(zarrArray);
        } else {
            final ProductWriter writer = createBinaryProductWriter();
            final Product binaryProduct = new Product("_" + name + "_", "binary", shape[IDX_WIDTH], shape[IDX_HEIGHT]);
            final GeoCoding geoCoding = band.getGeoCoding();
            if (geoCoding instanceof CrsGeoCoding) {
                binaryProduct.setSceneGeoCoding(geoCoding);
            }
            final Band binaryBand = binaryProduct.addBand("data", getSnapDataType(zarrArray.getDataType()).getValue());
            final String fileExtension = binaryWriterPlugIn.getDefaultFileExtensions()[0];
            final Path path = outputRoot.resolve(name).resolve(name + fileExtension);
            writer.writeProductNodes(binaryProduct, path.toFile());
            binaryProduct.setProductWriter(writer);
            binaryWriter = new UserDefinedBinaryWriter(binaryBand);
        }
        zarrWriters.put(band, binaryWriter);
    }

    private int[] getShape(Band band) {
        return new int[]{band.getRasterHeight(), band.getRasterWidth()};
    }

    private void switchToIntermediateMode(ProductWriter writer) {
        final Class<? extends ProductWriter> writerClass = writer.getClass();
        if (!writerClass.getName().contains("BigGeoTiffProductWriter")) {
            return;
        }
        try {
            final Method setWriteIntermediateProduct = writerClass.getDeclaredMethod("setWriteIntermediateProduct", boolean.class);
            setWriteIntermediateProduct.setAccessible(true);
            setWriteIntermediateProduct.invoke(writer, true);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "Unable to switch on 'writeIntermediateProduct' on binary product writer '" + binaryWriterPlugIn.getFormatNames()[0] + "'.", e);
        }
    }

    private Map<String, Object> collectBandAttributes(Band band) {
        final Map<String, Object> bandAttributes = new HashMap<>();
        collectBandAttributes(band, bandAttributes);
        collectSampleCodingAttributes(band, bandAttributes);
        collectVirtualBandAttributes(band, bandAttributes);
        collectRasterAttributes(band, bandAttributes);
        if (binaryWriterPlugIn != null) {
            bandAttributes.put(ATT_NAME_BINARY_FORMAT, binaryWriterPlugIn.getFormatNames()[0]);
        }
        final GeoCoding geoCoding = band.getGeoCoding();
        if (geoCoding != band.getProduct().getSceneGeoCoding()) {
            bandAttributes.put(ATT_NAME_GEOCODING, getGeoCodingAttributes(geoCoding));
        }
        return bandAttributes;
    }

    static void collectVirtualBandAttributes(Band band, Map<String, Object> bandAttributes) {
        if (band instanceof VirtualBand) {
            final VirtualBand virtualBand = (VirtualBand) band;
            bandAttributes.put(VIRTUAL_BAND_EXPRESSION, virtualBand.getExpression());
        }
    }

    private ProductData readTiePointGridData(TiePointGrid tiePointGrid) throws IOException {
        final int gridWidth = tiePointGrid.getGridWidth();
        final int gridHeight = tiePointGrid.getGridHeight();
        ProductData productData = tiePointGrid.createCompatibleRasterData(gridWidth, gridHeight);
        getSourceProduct().getProductReader().readTiePointGridRasterData(tiePointGrid, 0, 0, gridWidth, gridHeight, productData,
                                                                         ProgressMonitor.NULL);
        return productData;
    }

    private static void collectNoDataValue(RasterDataNode rdNode, Map<String, Object> attributes) {
        int nodeDataType = rdNode.getDataType();
        // TODO: 22.07.2019 SE -- shall log10 scaled really be prohibited
        final Number noDataValue;
        if (!rdNode.isLog10Scaled()) {

            if (rdNode.getScalingFactor() != 1.0) {
                attributes.put(SCALE_FACTOR, rdNode.getScalingFactor());
            }
            if (rdNode.getScalingOffset() != 0.0) {
                attributes.put(ADD_OFFSET, rdNode.getScalingOffset());
            }
            noDataValue = rdNode.getNoDataValue();
        } else {
            // scaling information is not written anymore for log10 scaled bands
            // instead we always write geophysical values
            // we do this because log scaling is not supported by NetCDF-CF conventions
            noDataValue = rdNode.getGeophysicalNoDataValue();
        }
        if (noDataValue.doubleValue() != 0.0) {
            if (ProductData.isIntType(nodeDataType)) {
                final long longValue = noDataValue.longValue();
                if (ProductData.isUIntType(nodeDataType)) {
                    attributes.put(FILL_VALUE, longValue & 0xffffffffL);
                } else {
                    attributes.put(FILL_VALUE, longValue);
                }
            } else if (ProductData.TYPE_FLOAT64 == nodeDataType) {
                attributes.put(FILL_VALUE, noDataValue.doubleValue());
            } else {
                attributes.put(FILL_VALUE, noDataValue.floatValue());
            }
        }
        if (rdNode.isNoDataValueUsed()) {
            attributes.put(NO_DATA_VALUE_USED, rdNode.isNoDataValueUsed());
        }
    }

    private interface BinaryWriter {
        void write(ProductData data, int[] dataShape, int[] offset) throws IOException, InvalidRangeException;

        void dispose();
    }

    private static class StandardZarrChunksWriter implements BinaryWriter {
        private final ZarrArray zarrArray;

        public StandardZarrChunksWriter(ZarrArray zarrArray) {
            this.zarrArray = zarrArray;
        }

        @Override
        public void write(ProductData data, int[] dataShape, int[] offset) throws IOException, InvalidRangeException {
            zarrArray.write(data.getElems(), dataShape, offset);
        }

        @Override
        public void dispose() {
        }
    }

    private static class UserDefinedBinaryWriter implements BinaryWriter {
        private final Band binaryBand;

        public UserDefinedBinaryWriter(Band binaryBand) {
            this.binaryBand = binaryBand;
        }

        @Override
        public void write(ProductData data, int[] dataShape, int[] offset) throws IOException, InvalidRangeException {
            binaryBand.writeRasterData(offset[IDX_X], offset[IDX_Y], dataShape[IDX_WIDTH], dataShape[IDX_HEIGHT], data);
        }

        @Override
        public void dispose() {
            final Product product = binaryBand.getProduct();
            product.dispose();
        }
    }
}
