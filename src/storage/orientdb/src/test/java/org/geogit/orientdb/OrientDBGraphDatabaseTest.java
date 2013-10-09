package org.geogit.orientdb;

import org.geogit.api.GlobalInjectorBuilder;
import org.geogit.api.InjectorBuilder;
import org.geogit.api.Platform;
import org.geogit.api.TestPlatform;
import org.geogit.di.GeogitModule;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.GraphDatabaseTest;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;
import org.geogit.storage.memory.HeapObjectDatabse;
import org.geogit.storage.memory.HeapRefDatabase;
import org.geogit.storage.memory.HeapStagingDatabase;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;

public class OrientDBGraphDatabaseTest extends GraphDatabaseTest {

    @Override
    protected Injector createInjector() {
        Platform testPlatform = new TestPlatform(envHome);
        GlobalInjectorBuilder.builder = new TestInjectorBuilder(testPlatform);
        return GlobalInjectorBuilder.builder.build();
    }

    public class TestInjectorBuilder extends InjectorBuilder {

        Platform platform;

        public TestInjectorBuilder(Platform platform) {
            this.platform = platform;
        }

        @Override
        public Injector build() {
            return Guice.createInjector(Modules.override(new GeogitModule()).with(
                    new AbstractModule() {

                        @Override
                        protected void configure() {
                            bind(ObjectDatabase.class).to(HeapObjectDatabse.class).in(
                                    Scopes.SINGLETON);
                            bind(StagingDatabase.class).to(HeapStagingDatabase.class).in(
                                    Scopes.SINGLETON);
                            bind(RefDatabase.class).to(HeapRefDatabase.class).in(Scopes.SINGLETON);

                            bind(GraphDatabase.class).to(OrientDBGraphDatabase.class).in(
                                    Scopes.SINGLETON);
                        }
                    }));
        }
    }

}
