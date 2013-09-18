/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.api;

import org.geogit.di.GeogitModule;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class InjectorBuilder {

    public Injector build() {
        return Guice.createInjector(new GeogitModule());
    }

}
