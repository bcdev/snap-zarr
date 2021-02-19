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

import javax.imageio.stream.ImageOutputStream;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

public class MainWriteRaw {

    private static final Path evaluationRoot = Paths.get("D:/temp/zarr_evaluation/dev");
    private static final Path rawFile = evaluationRoot.resolve("raw");

    public static void main(String[] args) throws IOException {
//        final boolean random = true;
//        final int offset = 0;
//        final boolean random = false;
//        final int offset = 126;
        final boolean random = false;
        final int offset = 0;
        writeBinaryFile(random, offset);

    }

    private static void writeBinaryFile(boolean random, int offset) throws IOException {
        final byte[] bytes = new byte[Double.BYTES * 2000 * 2000];
        if (random) {
            new Random().nextBytes(bytes);
        } else {
            for (int i = 0; i < bytes.length; i++) {
                final int value = i + offset;
                final byte bVal = (byte) value;
                bytes[i] = bVal;
            }
        }
        Files.createDirectories(evaluationRoot);
        final OutputStream outputStream = Files.newOutputStream(rawFile);
        final ImageOutputStream ios = new MemoryCacheImageOutputStream(outputStream);
        ios.write(bytes);
        ios.close();
    }

}
