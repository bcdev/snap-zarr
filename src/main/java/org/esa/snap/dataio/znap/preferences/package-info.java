/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
@OptionsPanelController.ContainerRegistration(
        id = "SNAP-ZARR",
        categoryName = "#OptionsCategory_Name_WriterOptionsPanel",
        iconBase = "org/esa/snap/dataio/znap/images/SNAP_data_32.png",
        keywords = "#OptionsCategory_Keywords_WriterOptionsPanel",
        keywordsCategory = "WriterOptionsPanel"
)
@NbBundle.Messages(value = {
        "OptionsCategory_Name_WriterOptionsPanel=SNAP-ZARR",
        "OptionsCategory_Keywords_WriterOptionsPanel=data"
})
package org.esa.snap.dataio.znap.preferences;

import org.netbeans.spi.options.OptionsPanelController;
import org.openide.util.NbBundle;
