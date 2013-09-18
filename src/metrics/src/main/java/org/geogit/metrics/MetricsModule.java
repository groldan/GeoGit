package org.geogit.metrics;

import com.google.common.util.concurrent.Service;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.palominolabs.metrics.guice.InstrumentationModule;

public class MetricsModule extends InstrumentationModule {

    @Override
    protected void configure() {
        //super.configure();
        Multibinder<Service> services = Multibinder.newSetBinder(binder(), Service.class);
        services.addBinding().to(MetricsService.class).in(Scopes.SINGLETON);
    }
}
