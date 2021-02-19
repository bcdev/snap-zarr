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
package org.esa.snap.dataio.znap.temp;

import com.bc.zarr.*;
import com.bc.zarr.storage.FileSystemStore;
import ucar.ma2.InvalidRangeException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ZarrWriteTestIncludingAttributes {

    public static void main(String[] args) throws IOException, InvalidRangeException {
        final Path rootPath = Paths.get("D:/temp/zarr_evaluation/dev/j/");

        final int width = 5;
        final int height = 3;
        final int[] shape = {height, width}; // common data model manner { y, x }

        final HashMap<String, Object> attributes = createMixedattributes();

        final int[] sourceBuffer = {
                10, 11, 12, 13, 14,
                15, 16, 17, 18, 19,
                20, 21, 22, 23, 24
        };

        final FileSystemStore store = new FileSystemStore(rootPath);
        final ZarrGroup zarrRoot = ZarrGroup.create(store, null);
        final Compressor compressor = CompressorFactory.create("zlib", 1);
        final ArrayParams arrayParameters = new ArrayParams()
                .dataType(DataType.i4).shape(shape).chunks(3, 3)
                .fillValue(-1).compressor(compressor);
        final ZarrArray zarrArray = zarrRoot.createArray("Attributes", arrayParameters, attributes);
        zarrArray.write(sourceBuffer, shape, new int[]{0, 0});
    }

    private static HashMap<String, Object> createMixedattributes() {
        final HashMap<String, Object> attributes = new HashMap<>();
        attributes.put("a", "was?");
        attributes.put("b", Arrays.asList(1.4, "e", false));
        attributes.put("c", 42);
        attributes.put("d", 16719875698374510761751.102870981375019857364518758134);
        final Map<String, Object> value = new HashMap<>();
        value.put("elsa", "friert");
        value.put("bei", 25);
        value.put("grad", "celsius");
        attributes.put("e", value);
        return attributes;
    }
}
