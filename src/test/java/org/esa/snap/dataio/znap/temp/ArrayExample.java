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

import ucar.ma2.Array;
import ucar.ma2.DataType;
import ucar.ma2.IndexIterator;
import ucar.ma2.InvalidRangeException;


public class ArrayExample {

    public static void main(String[] args) throws InvalidRangeException {

        final int height = 1234;
        final int width = 209715;
//        final int width = 2097152;
        final int s = width * height;
//        final long s = (long)width * height;
        final Array array = Array.factory(DataType.BYTE, new int[]{height, width}, new byte[s]);
//        ByteBuffer bb = ByteBuffer.allocate(width);
//        final Array array = Array.factory(DataType.BYTE, new int[]{height,width}, bb);
        final long size = array.getSize();
        System.out.println("size = " + size);
        final IndexIterator indexIterator = array.getIndexIterator();
        byte b = 0;
        while (indexIterator.hasNext()) {
            indexIterator.setByteNext(b++);
        }

        final Array viewUL = array.sectionNoReduce(new int[]{0, 0}, new int[]{10, 10}, null).copy();
        final byte[] storage = (byte[]) viewUL.getStorage();
        System.out.println("storage = " + storage);
        for (int i = 0; i < storage.length; i++) {
            byte b1 = storage[i];
            System.out.print("" + b1 + ", ");
            ;
            if ((i + 1) % 10 == 0) {
                System.out.println();
            }
        }
    }
}
