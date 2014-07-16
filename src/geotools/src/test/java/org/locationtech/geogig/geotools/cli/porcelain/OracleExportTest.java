/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.locationtech.geogig.geotools.cli.porcelain;

import static org.locationtech.geogig.geotools.cli.porcelain.TestHelper.createTestFactory;

import java.util.Arrays;

import jline.UnsupportedTerminal;
import jline.console.ConsoleReader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.locationtech.geogig.api.porcelain.CommitOp;
import org.locationtech.geogig.cli.CommandFailedException;
import org.locationtech.geogig.cli.GeogitCLI;
import org.locationtech.geogig.cli.InvalidParameterException;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

public class OracleExportTest extends RepositoryTestCase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private GeogitCLI cli;

    @Override
    public void setUpInternal() throws Exception {
        ConsoleReader consoleReader = new ConsoleReader(System.in, System.out,
                new UnsupportedTerminal());
        cli = new GeogitCLI(consoleReader);

        cli.setGeogit(geogit);

        // Add points
        insertAndAdd(points1);
        insertAndAdd(points2);
        insertAndAdd(points3);

        geogit.command(CommitOp.class).call();

        // Add lines
        insertAndAdd(lines1);
        insertAndAdd(lines2);
        insertAndAdd(lines3);

        geogit.command(CommitOp.class).call();
    }

    @Override
    public void tearDownInternal() throws Exception {
        cli.close();
    }

    @Test
    public void testExport() throws Exception {

        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points", "Points");
        exportCommand.dataStoreFactory = createTestFactory();
        exportCommand.run(cli);
    }

    @Test
    public void testNullDataStore() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points", "Points");
        exportCommand.dataStoreFactory = TestHelper.createNullTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testNoArgs() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList();
        exportCommand.dataStoreFactory = TestHelper.createNullTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportToTableThatExists() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points", "table1");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(CommandFailedException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportToTableThatExistsWithOverwrite() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("WORK_HEAD:Points", "testTable");
        exportCommand.dataStoreFactory = createTestFactory();
        exportCommand.run(cli);

        exportCommand.args = Arrays.asList("Lines", "testTable");
        exportCommand.overwrite = true;
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNonexistentFeatureTypeTree() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("invalidType", "invalidTable");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNullTable() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points", null);
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithNullFeatureType() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("", "invalidTable");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithEmptyStringForFeatureType() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("", "invalidTable");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithEmptyStringForTable() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points", "");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

    @Test
    public void testExportWithFeatureNameInsteadOfType() throws Exception {
        OracleExport exportCommand = new OracleExport();
        exportCommand.args = Arrays.asList("Points/Points.1", "invalidTable");
        exportCommand.dataStoreFactory = createTestFactory();
        exception.expect(InvalidParameterException.class);
        exportCommand.run(cli);
    }

}