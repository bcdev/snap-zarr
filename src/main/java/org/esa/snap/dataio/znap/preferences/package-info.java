/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
@OptionsPanelController.ContainerRegistration(
        id = "WriterOptionsPanel",
        categoryName = "#OptionsCategory_Name_WriterOptionsPanel",
        iconBase = "org/esa/snap/dataio/znap/images/SNAP_data_32.png",
        keywords = "#OptionsCategory_Keywords_WriterOptionsPanel",
        keywordsCategory = "WriterOptionsPanel"
)
@NbBundle.Messages(value = {
        "OptionsCategory_Name_WriterOptionsPanel=SNAP-DATA",
        "OptionsCategory_Keywords_WriterOptionsPanel=data"
})
package org.esa.snap.dataio.znap.preferences;

import org.netbeans.spi.options.OptionsPanelController;
import org.openide.util.NbBundle;


//@ContainerRegistration(
//        id = "SnapData",
//        categoryName = "#OptionsCategory_Name_SnapData",
//        iconBase = "org/esa/snap/dataio/znap/images/SNAP_data_32.jpg",
//        keywords = "#OptionsCategory_Keywords_SnapData",
//        keywordsCategory = "SnapData",
//        position = 500
//)
//@Messages(value = {
//        "OptionsCategory_Name_SnapData=SnapData",
//        "OptionsCategory_Keywords_SnapData=SnapData"
//})
//@ContainerRegistration(
//        id = "SNAP_Data",
//        categoryName = "#LBL_SNAPDataCategory_Name",
//        iconBase = "org/esa/snap/dataio/znap/images/SNAP_data_32.jpg",
//        keywords = "#LBL_SNAPDataCategory_Keywords",
//        keywordsCategory = "SNAP_Data",
//        position = 500
//)
//@Messages(value = {
//        "LBL_SNAPDataCategory_Name=SNAP_Data",
//        "LBL_SNAPDataCategory_Keywords=zarr"
//})
//package org.esa.snap.dataio.znap.preferences;
//
//import org.netbeans.spi.options.OptionsPanelController.ContainerRegistration;
//import org.openide.util.NbBundle.Messages;