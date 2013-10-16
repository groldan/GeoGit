/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */
package org.geogit.di;

import static com.google.inject.matcher.Matchers.subclassesOf;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Iterator;

import org.geogit.api.AbstractGeoGitOp;
import org.geogit.api.CommandLocator;
import org.geogit.api.DefaultPlatform;
import org.geogit.api.ObjectId;
import org.geogit.api.Platform;
import org.geogit.api.RevObject;
import org.geogit.repository.Index;
import org.geogit.repository.Repository;
import org.geogit.repository.StagingArea;
import org.geogit.repository.WorkingTree;
import org.geogit.storage.CachingObjectDatabaseGetInterceptor;
import org.geogit.storage.ConfigDatabase;
import org.geogit.storage.DeduplicationService;
import org.geogit.storage.GraphDatabase;
import org.geogit.storage.ObjectDatabase;
import org.geogit.storage.ObjectDatabasePutInterceptor;
import org.geogit.storage.ObjectSerializingFactory;
import org.geogit.storage.RefDatabase;
import org.geogit.storage.StagingDatabase;
import org.geogit.storage.TinkerGraphDatabase;
import org.geogit.storage.datastream.DataStreamSerializationFactory;
import org.geogit.storage.fs.FileObjectDatabase;
import org.geogit.storage.fs.FileRefDatabase;
import org.geogit.storage.fs.IniConfigDatabase;
import org.geogit.storage.memory.HeapDeduplicationService;
import org.geogit.storage.memory.HeapStagingDatabase;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.matcher.Matcher;
import com.google.inject.multibindings.Multibinder;

/**
 * Provides bindings for GeoGit singletons.
 * 
 * @see CommandLocator
 * @see Platform
 * @see Repository
 * @see ConfigDatabase
 * @see StagingArea
 * @see WorkingTree
 * @see ObjectDatabase
 * @see StagingDatabase
 * @see RefDatabase
 * @see GraphDatabase
 * @see ObjectSerializingFactory
 * @see DeduplicationService
 */

public class GeogitModule extends AbstractModule {

    /**
     * 
     * @see com.google.inject.AbstractModule#configure()
     */
    @Override
    protected void configure() {
        // empty set binding for Service instances managed by the repo life cycle, so that other
        // modules can contribute service bindings but if not the Repository constructor gets an
        // empty set
        Multibinder.newSetBinder(binder(), Service.class);

        bind(CommandLocator.class).to(GuiceCommandLocator.class).in(Scopes.SINGLETON);

        bind(Platform.class).to(DefaultPlatform.class).asEagerSingleton();

        bind(Repository.class).in(Scopes.SINGLETON);
        bind(ConfigDatabase.class).to(IniConfigDatabase.class).in(Scopes.SINGLETON);
        bind(StagingArea.class).to(Index.class).in(Scopes.SINGLETON);
        bind(StagingDatabase.class).to(HeapStagingDatabase.class).in(Scopes.SINGLETON);
        bind(WorkingTree.class).in(Scopes.SINGLETON);
        bind(GraphDatabase.class).to(TinkerGraphDatabase.class).in(Scopes.SINGLETON);

        bind(ObjectDatabase.class).to(FileObjectDatabase.class).in(Scopes.SINGLETON);
        bind(RefDatabase.class).to(FileRefDatabase.class).in(Scopes.SINGLETON);

        bind(ObjectSerializingFactory.class).to(DataStreamSerializationFactory.class).in(
                Scopes.SINGLETON);

        bind(DeduplicationService.class).to(HeapDeduplicationService.class).in(Scopes.SINGLETON);

        bindRevObjectCachingDatabaseInterceptor();

        bindCommitGraphInterceptor();

        bindConflictCheckingInterceptor();
    }

    private void bindRevObjectCachingDatabaseInterceptor() {
        final Method getObjectId;
        final Method getObjectIdClass;
        try {
            getObjectId = ObjectDatabase.class.getMethod("get", ObjectId.class);
            getObjectIdClass = ObjectDatabase.class.getMethod("get", ObjectId.class, Class.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        Matcher<Method> methodMatcher = new Matcher<Method>() {

            @Override
            public boolean matches(Method t) {
                if ("get".equals(t.getName())) {
                    if (Arrays.equals(getObjectId.getParameterTypes(), t.getParameterTypes())
                            || Arrays.equals(getObjectIdClass.getParameterTypes(),
                                    t.getParameterTypes())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Matcher<Method> and(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Matcher<Method> or(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }
        };

        bindInterceptor(subclassesOf(ObjectDatabase.class), methodMatcher,
                new CachingObjectDatabaseGetInterceptor());

    }

    private void bindConflictCheckingInterceptor() {
        final Method callMethod;
        try {
            callMethod = AbstractGeoGitOp.class.getMethod("call");
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        Matcher<Method> callMatcher = new Matcher<Method>() {

            @Override
            public boolean matches(Method t) {
                if (!t.isSynthetic()) {
                    if (callMethod.getName().equals(t.getName())) {
                        if (Arrays.equals(callMethod.getParameterTypes(), t.getParameterTypes())) {
                            return true;
                        }
                    }
                }
                return false;
            }

            @Override
            public Matcher<Method> and(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Matcher<Method> or(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }
        };

        Matcher<Class<?>> canRunDuringCommitMatcher = new Matcher<Class<?>>() {

            @Override
            public boolean matches(Class<?> clazz) {
                // TODO: this is not a very clean way of doing this...
                return !(clazz.getPackage().getName().contains("plumbing") || clazz
                        .isAnnotationPresent(CanRunDuringConflict.class));
            }

            @Override
            public Matcher<Class<?>> or(Matcher<? super Class<?>> arg0) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Matcher<Class<?>> and(Matcher<? super Class<?>> arg0) {
                throw new UnsupportedOperationException();
            }

        };

        bindInterceptor(canRunDuringCommitMatcher, callMatcher, new ConflictInterceptor());
    }

    private void bindCommitGraphInterceptor() {
        final Method putRevObject;
        final Method putAll;
        try {
            putRevObject = ObjectDatabase.class.getMethod("put", RevObject.class);
            putAll = ObjectDatabase.class.getMethod("putAll", Iterator.class);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        Matcher<Method> methodMatcher = new Matcher<Method>() {

            @Override
            public boolean matches(Method t) {
                if ("put".equals(t.getName())) {
                    if (Arrays.equals(putRevObject.getParameterTypes(), t.getParameterTypes())) {
                        return true;
                    }
                } else if ("putAll".equals(t.getName())) {
                    if (Arrays.equals(putAll.getParameterTypes(), t.getParameterTypes())) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Matcher<Method> and(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Matcher<Method> or(Matcher<? super Method> other) {
                throw new UnsupportedOperationException();
            }
        };

        bindInterceptor(subclassesOf(ObjectDatabase.class), methodMatcher,
                new ObjectDatabasePutInterceptor(getProvider(GraphDatabase.class)));
    }
}
