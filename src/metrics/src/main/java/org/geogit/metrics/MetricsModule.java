/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.metrics;

import com.google.inject.Scopes;
import com.palominolabs.metrics.guice.InstrumentationModule;

public class MetricsModule extends InstrumentationModule {

    @Override
    protected void configure() {
        super.configure();
        bind(MetricsLocationResolver.class).in(Scopes.SINGLETON);
    }
}
