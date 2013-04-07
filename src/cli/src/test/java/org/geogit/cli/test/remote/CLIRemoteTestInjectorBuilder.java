/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli.test.remote;

import java.io.File;

import org.geogit.api.InjectorBuilder;
import org.geogit.cli.test.functional.TestPlatform;
import org.geogit.di.GeogitModule;
import org.geogit.storage.bdbc.BDBStorageModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class CLIRemoteTestInjectorBuilder extends InjectorBuilder {

    File workingDirectory;

    File homeDirectory;

    public CLIRemoteTestInjectorBuilder(File workingDirectory, File homeDirectory) {
        this.workingDirectory = workingDirectory;
        this.homeDirectory = homeDirectory;
    }

    @Override
    public Injector build() {
        TestPlatform testPlatform = new TestPlatform(workingDirectory, homeDirectory);
        BDBStorageModule jeStorageModule = new BDBStorageModule();
        RemoteFunctionalTestModule functionalTestModule = new RemoteFunctionalTestModule(
                testPlatform);

        return Guice.createInjector(Modules.override(new GeogitModule()).with(jeStorageModule,
                functionalTestModule));
    }

}
