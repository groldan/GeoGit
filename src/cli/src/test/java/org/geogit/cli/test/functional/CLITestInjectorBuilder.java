/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.cli.test.functional;

import java.io.File;

import org.geogit.api.InjectorBuilder;
import org.geogit.di.GeogitModule;
import org.geogit.metrics.MetricsModule;
import org.geogit.test.integration.je.JETestStorageModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class CLITestInjectorBuilder extends InjectorBuilder {

    File workingDirectory;

    File homeDirectory;

    public CLITestInjectorBuilder(File workingDirectory, File homeDirectory) {
        this.workingDirectory = workingDirectory;
        this.homeDirectory = homeDirectory;
    }

    @Override
    public Injector build() {
        TestPlatform testPlatform = new TestPlatform(workingDirectory, homeDirectory);
        JETestStorageModule jeStorageModule = new JETestStorageModule();
        FunctionalTestModule functionalTestModule = new FunctionalTestModule(testPlatform);

        Injector injector = Guice.createInjector(Modules.override(new GeogitModule()).with(
                jeStorageModule, functionalTestModule, new MetricsModule()));
        return injector;
    }

}
