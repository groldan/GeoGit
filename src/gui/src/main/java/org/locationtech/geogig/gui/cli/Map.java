package org.locationtech.geogig.gui.cli;

import java.io.IOException;
import java.util.List;

import org.geogit.gui.internal.MapPane;
import org.locationtech.geogig.api.GeoGIG;
import org.locationtech.geogig.cli.AbstractCommand;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.GeogigCLI;
import org.locationtech.geogig.cli.annotation.RequiresRepository;
import org.locationtech.geogig.repository.Hints;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Lists;

@RequiresRepository(true)
@Parameters(commandNames = "map", commandDescription = "Opens a map")
public class Map extends AbstractCommand {

    @Parameter(description = "<layer names>,...")
    private List<String> layerNames = Lists.newArrayList();

    @Override
    protected void runInternal(GeogigCLI cli) {
        GeoGIG geogig = cli.newGeoGIG(Hints.readOnly());
        MapPane mapPane;
        try {
            mapPane = new MapPane(geogig, layerNames);
            mapPane.show();
            cli.setExitOnFinish(false);
        } catch (IOException e) {
            throw new CommandFailedException(e);
        }
    }

}
