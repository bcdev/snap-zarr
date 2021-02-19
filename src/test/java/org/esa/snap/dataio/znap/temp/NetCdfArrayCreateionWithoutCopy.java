package org.esa.snap.dataio.znap.temp;

import com.bc.zarr.ucar.NetCDF_Util;
import ucar.ma2.Array;

import java.lang.*;

public class NetCdfArrayCreateionWithoutCopy {

    public static void main(String[] args) {
        final int[] ints = {1, 2, 3, 4, 5, 6};
        System.out.println("ints = " + ints);
        final int[] shape = {2, 3};
        final Array a = NetCDF_Util.createArrayWithGivenStorage(ints, shape);

        System.out.println("ints = " + a.getStorage());
    }


}
