/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.test.integration.je.repository;

import org.locationtech.geogig.api.Context;
import org.locationtech.geogig.di.GeogitModule;
import org.locationtech.geogig.test.integration.je.JETestStorageModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class JEIndexTest extends org.locationtech.geogig.test.integration.repository.IndexTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new JETestStorageModule())).getInstance(
                Context.class);
    }
}