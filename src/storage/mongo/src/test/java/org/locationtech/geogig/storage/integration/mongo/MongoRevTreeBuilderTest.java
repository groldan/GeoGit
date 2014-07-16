/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.locationtech.geogig.storage.integration.mongo;

import org.junit.Test;
import org.locationtech.geogig.api.Context;
import org.locationtech.geogig.di.GeogitModule;

import com.google.inject.Guice;
import com.google.inject.util.Modules;

public class MongoRevTreeBuilderTest extends
        org.locationtech.geogig.test.integration.RevTreeBuilderTest {
    @Override
    protected Context createInjector() {
        return Guice.createInjector(
                Modules.override(new GeogitModule()).with(new MongoTestStorageModule()))
                .getInstance(Context.class);
    }

    @Test
    // $codepro.audit.disable unnecessaryOverride
    public void testPutIterate() throws Exception {
        super.testPutIterate();
    }

    @Test
    // $codepro.audit.disable unnecessaryOverride
    public void testPutRandomGet() throws Exception {
        super.testPutRandomGet();
    }

    public static void main(String... args) {
        MongoRevTreeBuilderTest test = new MongoRevTreeBuilderTest();
        try {
            test.setUp();
            test.testPutRandomGet();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}