/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.test.integration.bdbc;

import java.io.File;

import org.geogit.api.Platform;
import org.geogit.api.TestPlatform;
import org.geogit.di.GeogitModule;
import org.geogit.storage.bdbc.BDBStorageModule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class DBCommitOpTest extends org.geogit.test.integration.CommitOpTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Override
    protected Injector createInjector() {
        File workingDirectory;
        try {
            workingDirectory = tempFolder.newFolder("mockWorkingDir");
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        Platform testPlatform = new TestPlatform(workingDirectory);
        return Guice.createInjector(Modules.override(new GeogitModule()).with(
                new BDBStorageModule(), new TestModule(testPlatform)));
    }

}
