/*
 * $Id$
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
package org.esa.snap.dataio.znap.snap;

import com.bc.zarr.DataType;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

final class ZnapConstantsAndUtils {

    public static final String PROPERTY_NAME_COMPRESSON_LEVEL = "znap.compresson.level";
    public static final String PROPERTY_NAME_MAX_WRITE_THREADS = "znap.num.write.threads";
    public static final String PROPERTY_NAME_BINARY_FORMAT = "znap.binary.format";

    public static final int IDX_WIDTH = 1;
    public static final int IDX_X = IDX_WIDTH;
    public static final int IDX_HEIGHT = 0;
    public static final int IDX_Y = IDX_HEIGHT;

    static final String FORMAT_NAME = "SNAP-Zarr";
    static final String SNAP_ZARR_CONTAINER_EXTENSION = ".znap";

    public static final String UNIT_EXTENSION = "_unit";
    public static final String BANDWIDTH = "bandwidth";
    public static final String BANDWIDTH_UNIT = "nm";
    public static final String WAVELENGTH = "wavelength";
    public static final String WAVELENGTH_UNIT = "nm";
    public static final String VALID_PIXEL_EXPRESSION = "valid_pixel_expression";
    public static final String QUICKLOOK_BAND_NAME = "quicklook_band_name";
    public static final String SOLAR_FLUX = "solar_flux";
    public static final String SPECTRAL_BAND_INDEX = "spectral_band_index";

    public static final String NO_DATA_VALUE_USED = "no_data_value_used";

    public static final String ATT_NAME_BINARY_FORMAT = PROPERTY_NAME_BINARY_FORMAT.replace(".", "_");
    public static final String ATT_NAME_GEOCODING = "geocoding";
    public static final String ATT_NAME_GEOCODING_SHARED = "shared";
    public static final int LON_INDEX = 0;
    public static final int LAT_INDEX = 1;

    // Product attributes
    public static final String DATASET_AUTO_GROUPING = "dataset_auto_grouping";

    // Tie point grid attributes
    public static final String DISCONTINUITY = "discontinuity";

    // Sample coding attributes
    public static final String FLAG_DESCRIPTIONS = "flag_descriptions";

    // Product header keys
    public static final String ATT_NAME_PRODUCT_NAME = "product_name";
    public static final String ATT_NAME_PRODUCT_TYPE = "product_type";
    public static final String ATT_NAME_PRODUCT_DESC = "product_description";
    public static final String ATT_NAME_PRODUCT_METADATA = "product_metadata";

    // Tie point grid attribute names
    public static final String ATT_NAME_OFFSET_X = "offset_x";
    public static final String ATT_NAME_OFFSET_Y = "offset_y";
    public static final String ATT_NAME_SUBSAMPLING_X = "subsampling_x";
    public static final String ATT_NAME_SUBSAMPLING_Y = "subsampling_y";

    // Sample Coding
    public static final String NAME_SAMPLE_CODING = "name_sample_coding";

    static final Class[] IO_TYPES = new Class[]{
            Path.class,
            File.class,
            String.class
    };

    private static final OutputConverter[] IO_CONVERTERS = new OutputConverter[]{
            output -> (Path) output,
            output -> ((File) output).toPath(),
            output -> Paths.get((String) output)
    };

    static Path convertToPath(final Object object) {
        for (int i = 0; i < IO_TYPES.length; i++) {
            if (IO_TYPES[i].isInstance(object)) {
                return IO_CONVERTERS[i].convertOutput(object);
            }
        }
        return null;
    }

    private interface OutputConverter {

        Path convertOutput(Object output);
    }

    static SnapDataType getSnapDataType(DataType zarrDataType) {
        if (zarrDataType == DataType.f8) {
            return SnapDataType.TYPE_FLOAT64;
        } else if (zarrDataType == DataType.f4) {
            return SnapDataType.TYPE_FLOAT32;
        } else if (zarrDataType == DataType.i1) {
            return SnapDataType.TYPE_INT8;
        } else if (zarrDataType == DataType.u1) {
            return SnapDataType.TYPE_UINT8;
        } else if (zarrDataType == DataType.i2) {
            return SnapDataType.TYPE_INT16;
        } else if (zarrDataType == DataType.u2) {
            return SnapDataType.TYPE_UINT16;
        } else if (zarrDataType == DataType.i4) {
            return SnapDataType.TYPE_INT32;
        } else if (zarrDataType == DataType.u4) {
            return SnapDataType.TYPE_UINT32;
        } else {
            throw new IllegalStateException();
        }
    }
}
