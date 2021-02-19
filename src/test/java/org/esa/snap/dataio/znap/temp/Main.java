/*
 * $Id$
 *
 * Copyright (C) 2020 by Brockmann Consult (info@brockmann-consult.de)
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
import org.esa.snap.core.datamodel.RasterDataNode;
import org.esa.snap.dataio.znap.snap.ZarrProductWriter;
import org.esa.snap.dataio.znap.snap.ZarrProductWriterPlugIn;
import org.esa.snap.dataio.znap.snap._Helper;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class Main {

    public static void main(String[] args) throws IOException {
        final Path EVALUATION_ROOT = Paths.get("D:/temp/zarr_evaluation/dev");
//        final Path rawFile = EVALUATION_ROOT.resolve("raw");
        final Path rawFile = null;


        final String productName = "ProductName";
        final String productType = "TestType";
        final int[] shape = {17, 4};   // SNAP manner { x, y }
        final int[] chunks = {16, 3};   // SNAP manner { x, y }
//        final int[] shape = {15, 10};   // SNAP manner { x, y }
//        final int[] chunks = {8, 6};   // SNAP manner { x, y }
        final Product product = ProductFactory.create(productName, productType, shape, chunks, rawFile);
//        printBandValues(product);
        final ZarrProductWriterPlugIn zarrProductWriterPlugIn = new ZarrProductWriterPlugIn();
        System.setProperty(_Helper.PROPERTY_NAME_COMPRESSON_LEVEL, "3");
        final ZarrProductWriter productWriter = (ZarrProductWriter) zarrProductWriterPlugIn.createWriterInstance();

        final Path snap = EVALUATION_ROOT.resolve("snap");
//        productWriter.setCompressor(Compressor.Null);
//        final Path snap = EVALUATION_ROOT.resolve("snap_flat");

        product.setProductWriter(productWriter);
        final String output = snap.toString();
        productWriter.writeProductNodes(product, output);
        final List<RasterDataNode> rasterDataNodes = product.getRasterDataNodes();
        for (RasterDataNode node : rasterDataNodes) {
            System.out.println("##################################################################################################");
            node.writeRasterDataFully();
        }
    }

    private static void printBandValues(Product product) {
        final Band[] bands = product.getBands();
        for (Band band : bands) {
            System.out.println();
            System.out.println(band.getDisplayName());
            final ProductData data = band.getData();
            int count = data.getNumElems();
            final int last = count - 1;
            System.out.print("[");
            for (int i = 0; i < count; i++) {
                final String elem = data.getElemStringAt(i);
                System.out.print(elem);
                if (i < last) {
                    System.out.print(" ");
                }
            }
            System.out.println("]");
        }
    }
}
