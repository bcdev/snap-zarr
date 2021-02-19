/*
 * $
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
import com.bc.zarr.chunk.ChunkReaderWriter;
import com.bc.zarr.chunk.ChunkReaderWriterImpl_Integer;
import com.bc.zarr.storage.FileSystemStore;
import ucar.ma2.Array;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CreateChunks {

    public static void main(String[] args) throws IOException {
        final Path[] paths = {
                Paths.get("D:\\temp\\zarr_evaluation\\Products\\zarr\\Tiles70x100.znap\\band370x530chunks70x100_compressed"),
                Paths.get("D:\\temp\\zarr_evaluation\\Products\\zarr\\Tiles70x100.znap\\band370x530chunks70x100_uncompressed")
        };

        for (Path path : paths) {
            final FileSystemStore store = new FileSystemStore(path);
            final Path headerPath = path.resolve(ZarrConstants.FILENAME_DOT_ZARRAY);
            final ZarrHeader zarrHeader;
            try (BufferedReader reader = Files.newBufferedReader(headerPath)) {
                zarrHeader = ZarrUtils.fromJson(reader, ZarrHeader.class);
            }
            final int[] chunks = zarrHeader.getChunks();
            final int[] shape = zarrHeader.getShape();
            Compressor compressor = zarrHeader.getCompressor();
            if (compressor == null) {
                compressor = CompressorFactory.nullCompressor;
            }


            final int[][] indices = ZarrUtils.computeChunkIndices(shape, chunks, shape, new int[]{0, 0});
            for (int i = 0; i < indices.length; i++) {
                int[] chunkIndex = indices[i];
                final String chunkFilename = ZarrUtils.createChunkFilename(chunkIndex);
                final Path chunkFilePath = path.resolve(chunkFilename);
                final ChunkReaderWriter chunkReaderWriter = new ChunkReaderWriterImpl_Integer(ByteOrder.BIG_ENDIAN, compressor, chunks, i, store);
                final Array read = chunkReaderWriter.read(chunkFilename);
                chunkReaderWriter.write(chunkFilename, read);
                System.out.println("Chunk '" + chunkFilename + "' written with compressor '" + compressor.getId() + "' with value " + i);
            }
        }
    }
}
