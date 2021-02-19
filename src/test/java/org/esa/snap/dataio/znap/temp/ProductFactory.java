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

import org.esa.snap.core.datamodel.Band;
import org.esa.snap.core.datamodel.Product;
import org.esa.snap.core.datamodel.ProductData;

import javax.imageio.stream.MemoryCacheImageInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.esa.snap.core.datamodel.ProductData.*;

public class ProductFactory {


    public static Product create(String name, final String type, int[] shape, int[] chunks, Path rawFile) throws IOException {
        final int sceneRasterWidth = shape[0];
        final int sceneRasterHeight = shape[1];
        final Product product = new Product(name, type, sceneRasterWidth, sceneRasterHeight);
        product.setPreferredTileSize(chunks[0], chunks[1]);


        final int[] dataTypes = {
                TYPE_FLOAT64,
                TYPE_FLOAT32,
                TYPE_INT32,
                TYPE_UINT32,
                TYPE_INT16,
                TYPE_UINT16,
                TYPE_INT8,
                TYPE_UINT8
        };

        if (rawFile != null) {
            final int rawSize = (int) Files.size(rawFile);
            final byte[] rawBytes = new byte[rawSize];
            final InputStream inputStream = Files.newInputStream(rawFile);
            inputStream.read(rawBytes);
            final ByteArrayInputStream is = new ByteArrayInputStream(rawBytes);

            for (int dataType : dataTypes) {
                final MemoryCacheImageInputStream ciis = new MemoryCacheImageInputStream(is);
                ciis.setByteOrder(ByteOrder.BIG_ENDIAN);
                addBand(product, ProductData.getTypeString(dataType), dataType, ciis);
                is.reset();
                System.out.println();
            }
        } else {
            for (int dataType : dataTypes) {
                final String typeString = ProductData.getTypeString(dataType);
                final Band band = product.addBand(typeString, dataType);
                band.ensureRasterData();
                long size = sceneRasterWidth * sceneRasterHeight;
                for (int i = 0; i < size; i++) {
                    final int x = i % sceneRasterWidth;
                    final int y = i / sceneRasterWidth;
                    band.setPixelInt(x, y, i);
                }
//                MultiLevelImage mli = band.getSourceImage();
//                band.setSourceImage(mli);
            }
        }
        return product;
    }

    private static Band addBand(Product product, String bandName, int dataType, MemoryCacheImageInputStream cacheImageInputStream) throws IOException {
        final Band band = product.addBand(bandName, dataType);
        final ProductData data = band.createCompatibleRasterData();
        data.readFrom(cacheImageInputStream);
        band.setData(data);
        return band;
    }
}
