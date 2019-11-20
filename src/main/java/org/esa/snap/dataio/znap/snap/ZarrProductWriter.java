package org.esa.snap.dataio.znap.snap;

import com.bc.ceres.core.ProgressMonitor;
import com.bc.ceres.glevel.MultiLevelImage;
import com.bc.zarr.*;
import org.esa.snap.core.dataio.AbstractProductWriter;
import org.esa.snap.core.dataio.ProductIO;
import org.esa.snap.core.dataio.ProductWriter;
import org.esa.snap.core.dataio.ProductWriterPlugIn;
import org.esa.snap.core.datamodel.*;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.esa.snap.core.util.StringUtils.isNotNullAndNotEmpty;
import static org.esa.snap.core.util.SystemUtils.LOG;
import static org.esa.snap.dataio.znap.snap.CFConstantsAndUtils.*;
import static org.esa.snap.dataio.znap.snap.ZnapConstantsAndUtils.*;
import static ucar.nc2.constants.CDM.TIME_END;
import static ucar.nc2.constants.CDM.TIME_START;

public class ZarrProductWriter extends AbstractProductWriter {

    public static final int DEFAULT_NUM_THREADS = 1;

    private final HashMap<Band, BinaryWriter> zarrWriters = new HashMap<>();
    private final Compressor compressor;
    private final ThreadPoolExecutor executorService;
    private final ProductWriterPlugIn binaryWriterPlugIn;
    private ZarrGroup zarrGroup;
    private Path outputRoot;

    public ZarrProductWriter(final ZarrProductWriterPlugIn productWriterPlugIn) {
        super(productWriterPlugIn);
        compressor = CompressorFactory.create("zlib", getCompressionLevel(3));
        final int threads = getNumWritingThreads(DEFAULT_NUM_THREADS);
        executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads > 0 ? threads : DEFAULT_NUM_THREADS);
        binaryWriterPlugIn = getBinaryWriterPlugin(productWriterPlugIn);
    }

    @Override
    public void writeBandRasterData(Band sourceBand, int sourceOffsetX, int sourceOffsetY, int sourceWidth, int sourceHeight, ProductData sourceBuffer, ProgressMonitor pm) throws IOException {
        final BinaryWriter binaryWriter = zarrWriters.get(sourceBand);
        final int[] to = {sourceOffsetY, sourceOffsetX}; // common data model manner { y, x }
        final int[] shape = {sourceHeight, sourceWidth};  // common data model manner { y, x }
        Callable<Object> callable = () -> {
            try {
                binaryWriter.write(ensureNotLogarithmicData(sourceBand, sourceBuffer), shape, to);
                return null;
            } catch (InvalidRangeException e) {
                throw new IOException("Invalid range while writing raster '" + sourceBand.getName() + "'", e);
            }
        };
        executorService.submit(callable);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() throws IOException {
        try {
            executorService.shutdown();
            executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        for (BinaryWriter value : zarrWriters.values()) {
            value.dispose();
        }
        zarrWriters.clear();
    }

    @Override
    public void deleteOutput() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    protected void writeProductNodesImpl() throws IOException {
        outputRoot = convertToPath(getOutput());
        final Product product = getSourceProduct();

        zarrGroup = ZarrGroup.create(outputRoot, collectProductAttributes(product));
        for (TiePointGrid tiePointGrid : product.getTiePointGrids()) {
            writeTiePointGrid(tiePointGrid);
        }
        for (Band band : product.getBands()) {
            initializeZarrBandWriter(band);
        }
    }

    static Map<String, Object> collectProductAttributes(Product product) {
        final Map<String, Object> productAttributes = new HashMap<>();
        productAttributes.put(PRODUCT_NAME, product.getName());
        productAttributes.put(PRODUCT_TYPE, product.getProductType());
        productAttributes.put(PRODUCT_DESC, product.getDescription());
        productAttributes.put(TIME_START, ISO8601ConverterWithMlliseconds.format(product.getStartTime())); // "time_coverage_start"
        productAttributes.put(TIME_END, ISO8601ConverterWithMlliseconds.format(product.getEndTime())); // "time_coverage_end"

        final boolean prettyPrinting = false;
        final String metadataNotPrettyPrint = ZarrUtils.toJson(product.getMetadataRoot().getElements(), prettyPrinting);
        productAttributes.put(PRODUCT_METADATA, metadataNotPrettyPrint);

        if (product.getAutoGrouping() != null) {
            productAttributes.put(DATASET_AUTO_GROUPING, product.getAutoGrouping().toString());
        }
        if (isNotNullAndNotEmpty(product.getQuicklookBandName())) {
            productAttributes.put(QUICKLOOK_BAND_NAME, product.getQuicklookBandName());
        }
        return productAttributes;
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

        collectSampleCodingAttributes(band, attributes);
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

    private static void collectSampleCodingAttributes(Band band, Map<String, Object> attributes) {
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
        if (indexBand) {
            attributes.put(FLAG_MEANINGS, names);
            attributes.put(FLAG_VALUES, values);
        } else {
            attributes.put(FLAG_MEANINGS, names);
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

    private int getCompressionLevel(int defaultCompressionLevel) {
        final String value = System.getProperty(PROPERTY_NAME_COMPRESSON_LEVEL, "" + defaultCompressionLevel);
        final int compressionLevel = Integer.parseInt(value);
        if (compressionLevel != defaultCompressionLevel) {
            LOG.info("Znap format product writer will use " + compressionLevel + " compression level.");
        }
        return compressionLevel;
    }

    private int getNumWritingThreads(int defaultValue) {
        final int maxNumWritingThreads = Integer.parseInt(System.getProperty(PROPERTY_NAME_MAX_WRITE_THREADS, "" + defaultValue));
        if (maxNumWritingThreads != defaultValue) {
            LOG.info("Znap format product writer will use " + maxNumWritingThreads + " threads to write data.");
        }
        return maxNumWritingThreads;
    }

    private ProductWriterPlugIn getBinaryWriterPlugin(ZarrProductWriterPlugIn productWriterPlugIn) {
        String defaultBinaryFormatName = productWriterPlugIn.getFormatNames()[0];
        final String binaryFormatName = System.getProperty(PROPERTY_NAME_BINARY_FORMAT, defaultBinaryFormatName);
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
        if (tiePointGrid.hasRasterData()) {
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
        attributes.put(OFFSET_X, tiePointGrid.getOffsetX());
        attributes.put(OFFSET_Y, tiePointGrid.getOffsetY());
        attributes.put(SUBSAMPLING_X, tiePointGrid.getSubSamplingX());
        attributes.put(SUBSAMPLING_Y, tiePointGrid.getSubSamplingY());
        if (binaryWriterPlugIn != null) {
            attributes.put(ATTRIBUTE_NAME_BINARY_FORMAT, binaryWriterPlugIn.getFormatNames()[0]);
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

    private void initializeZarrBandWriter(Band band) throws IOException {
        final int[] shape = {band.getRasterHeight(), band.getRasterWidth()}; // common data model manner { y, x }
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
        final ZarrArray zarrArray = zarrGroup.createArray(name, arrayParams, collectBandAttributes(band));
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
        collectRasterAttributes(band, bandAttributes);
        collectBandAttributes(band, bandAttributes);
        if (binaryWriterPlugIn != null) {
            bandAttributes.put(ATTRIBUTE_NAME_BINARY_FORMAT, binaryWriterPlugIn.getFormatNames()[0]);
        }
        return bandAttributes;
    }

    private ProductData readTiePointGridData(TiePointGrid tiePointGrid) throws IOException {
        final int gridWidth = tiePointGrid.getGridWidth();
        final int gridHeight = tiePointGrid.getGridHeight();
        ProductData productData = tiePointGrid.createCompatibleRasterData(gridWidth, gridHeight);
        getSourceProduct().getProductReader().readTiePointGridRasterData(tiePointGrid, 0, 0, gridWidth, gridHeight, productData,
                ProgressMonitor.NULL);
        return productData;
    }

    static void collectRasterAttributes(RasterDataNode rdNode, Map<String, Object> attributes) {

        final int nodeDataType = rdNode.getDataType();

        if (rdNode.getDescription() != null) {
            attributes.put(LONG_NAME, rdNode.getDescription());
        }
        if (rdNode.getUnit() != null) {
            attributes.put(UNITS, tryFindUnitString(rdNode.getUnit()));
        }
        if (ProductData.isUIntType(nodeDataType)) {
            attributes.put(UNSIGNED, String.valueOf(true));
        }
        collectNoDataValue(rdNode, attributes);

        if (isNotNullAndNotEmpty(rdNode.getValidPixelExpression())) {
            attributes.put(VALID_PIXEL_EXPRESSION, rdNode.getValidPixelExpression());
        }
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

    // TODO: 21.07.2019 SE implement geo coding
    // implement geocoding part for product
    // and for Band too

    //    private void encodeGeoCoding(NFileWriteable ncFile, Band band, Product product, NVariable variable) throws IOException {
//        final GeoCoding geoCoding = band.getGeoCoding();
//        if (!geoCoding.equals(product.getSceneGeoCoding())) {
//            if (geoCoding instanceof TiePointGeoCoding) {
//                final TiePointGeoCoding tpGC = (TiePointGeoCoding) geoCoding;
//                final String[] names = new String[2];
//                names[LON_INDEX] = tpGC.getLonGrid().getName();
//                names[LAT_INDEX] = tpGC.getLatGrid().getName();
//                final String value = StringUtils.arrayToString(names, " ");
//                variable.addAttribute(GEOCODING, value);
//            } else {
//                if (geoCoding instanceof CrsGeoCoding) {
//                    final CoordinateReferenceSystem crs = geoCoding.getMapCRS();
//                    final double[] matrix = new double[6];
//                    final MathTransform transform = geoCoding.getImageToMapTransform();
//                    if (transform instanceof AffineTransform) {
//                        ((AffineTransform) transform).getMatrix(matrix);
//                    }
//                    final String crsName = "crs_" + band.getName();
//                    final NVariable crsVariable = ncFile.addScalarVariable(crsName, DataType.INT);
//                    crsVariable.addAttribute("wkt", crs.toWKT());
//                    crsVariable.addAttribute("i2m", StringUtils.arrayToCsv(matrix));
//                    variable.addAttribute(GEOCODING, crsName);
//                }
//            }
//        }
//    }

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
