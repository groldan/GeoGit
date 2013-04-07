/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.test.integration.bdbc;

import org.geogit.di.GeogitModule;
import org.geogit.storage.bdbc.BDBStorageModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;

public class DBLogOpTest extends org.geogit.test.integration.LogOpTest {
    @Override
    protected Injector createInjector() {
        return Guice.createInjector(Modules.override(new GeogitModule())
                .with(new BDBStorageModule()));
    }
}
