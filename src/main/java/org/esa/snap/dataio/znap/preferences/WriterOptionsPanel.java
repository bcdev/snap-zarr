package org.esa.snap.dataio.znap.preferences;

import org.netbeans.spi.options.OptionsPanelController;
import org.openide.util.HelpCtx;
import org.openide.util.Lookup;

import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import java.awt.BorderLayout;
import java.beans.PropertyChangeListener;

@OptionsPanelController.SubRegistration(
        location = "SnapData",
        displayName = "#SNAPDataZarrOption_DisplayName",
        keywords = "#SNAPDataZarrOption_Keywords",
        keywordsCategory = "SnapData/SnapZarrOptions"
)
@org.openide.util.NbBundle.Messages({
        "SNAPDataZarrOption_DisplayName=SNAP-ZARR",
        "SNAPDataZarrOption_Keywords=zarr"
})
public class WriterOptionsPanel extends OptionsPanelController {
    @Override
    public void update() {

    }

    @Override
    public void applyChanges() {

    }

    @Override
    public void cancel() {

    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public boolean isChanged() {
        return false;
    }

    @Override
    public JComponent getComponent(Lookup masterLookup) {
        JPanel jPanel = new JPanel(new BorderLayout(10, 10));
        jPanel.add(new JLabel("ZARR"));
        return jPanel;
    }

    @Override
    public HelpCtx getHelpCtx() {
        return null;
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener l) {

    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener l) {

    }
}
