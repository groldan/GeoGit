package org.locationtech.geogig.geotools.render;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.locationtech.geogig.geotools.render.RenderSupport.buildMapTransform;
import static org.locationtech.geogig.geotools.render.RenderSupport.buildTransform;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import javax.annotation.Nullable;

import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.LiteCoordinateSequence;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.ScreenMap;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.StyleAttributeExtractor;
import org.geotools.styling.Symbolizer;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;

class RenderableFeature {
    private static final Logger LOGGER = LoggerFactory.getLogger(RenderableFeature.class);

    /**
     * A decimator that will just transform coordinates
     */
    private static final Decimator NULL_DECIMATOR = new Decimator(-1, -1);

    private final static FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2(null);

    private final static PropertyName defaultGeometryPropertyName = filterFactory.property("");

    private final IdentityHashMap<Symbolizer, SymbolizerAssociation> symbolizerAssociationHT = new IdentityHashMap<>();

    private final List<Geometry> geometries = new ArrayList<>(2);

    private final List<LiteShape2> shapes = new ArrayList<>(2);

    private final IdentityHashMap<MathTransform, Decimator> decimators = new IdentityHashMap<>();

    private final CoordinateReferenceSystem destinationCrs;

    private final AffineTransform worldToScreen;

    private ScreenMap screenMap;

    private final Rectangle screenSize;

    private Object content;

    private CoordinateReferenceSystem defaultNativeCrs;

    /** Maximum displacement for generalization during rendering */
    final static double generalizationDistance = 0.8;

    private boolean inMemoryGeneralization = true;

    public RenderableFeature(final AffineTransform worldToScreen,
            final CoordinateReferenceSystem destinationCrs, Rectangle screenSize) {
        checkNotNull(destinationCrs, "destinationCrs");
        checkNotNull(worldToScreen, "worldToScreen");
        this.worldToScreen = worldToScreen;
        this.destinationCrs = destinationCrs;
        this.screenSize = screenSize;
    }

    public void setFeature(Object feature, CoordinateReferenceSystem nativeCrs) {
        this.content = feature;
        this.defaultNativeCrs = nativeCrs;
        geometries.clear();
        shapes.clear();
    }

    public LiteShape2 getShape(final Symbolizer symbolizer, final AffineTransform at)
            throws FactoryException {

        Geometry g = findGeometry(content, symbolizer); // pulls the geometry

        if (g == null)
            return null;

        try {
            // process screenmap if necessary (only do it once,
            // the geometry will be transformed simplified in place and the screenmap
            // really needs to play against the original coordinates, plus, once we start
            // drawing a geometry we want to apply all symbolizers on it)
            if (screenMap != null //
                    && !(symbolizer instanceof PointSymbolizer) //
                    && !(g instanceof Point) && getGeometryIndex(g) == -1) {
                Envelope env = g.getEnvelopeInternal();
                if (screenMap.canSimplify(env))
                    if (screenMap.checkAndSet(env)) {
                        return null;
                    } else {
                        g = screenMap.getSimplifiedShape(env.getMinX(), env.getMinY(),
                                env.getMaxX(), env.getMaxY(), g.getFactory(), g.getClass());
                    }
            }

            SymbolizerAssociation sa = symbolizerAssociationHT.get(symbolizer);
            MathTransform crsTransform = null;
            MathTransform atTransform = null;
            MathTransform fullTransform = null;
            if (sa == null) {
                sa = new SymbolizerAssociation();
                sa.crs = (findGeometryCS(content, symbolizer));
                try {
                    crsTransform = buildTransform(sa.crs, destinationCrs);
                    atTransform = ProjectiveTransform.create(worldToScreen);
                    fullTransform = buildMapTransform(sa.crs, destinationCrs, at);
                } catch (Exception e) {
                    // fall through
                    LOGGER.warn(e.getLocalizedMessage(), e);
                }
                sa.xform = fullTransform;
                sa.crsxform = crsTransform;
                sa.axform = atTransform;
                symbolizerAssociationHT.put(symbolizer, sa);
            }

            // some shapes may be too close to projection boundaries to
            // get transformed, try to be lenient
            if (symbolizer instanceof PointSymbolizer) {
                // if the coordinate transformation will occurr in place on the coordinate
                // sequence
                if (g.getFactory().getCoordinateSequenceFactory() instanceof LiteCoordinateSequenceFactory) {
                    // if the symbolizer is a point symbolizer we first get the transformed
                    // geometry to make sure the coordinates have been modified once, and then
                    // compute the centroid in the screen space. This is a side effect of the
                    // fact we're modifing the geometry coordinates directly, if we don't get
                    // the reprojected and decimated geometry we risk of transforming it twice
                    // when computing the centroid
                    LiteShape2 first = getTransformedShape(g, sa);
                    if (first != null) {
                        return getTransformedShape(RendererUtilities.getCentroid(g), null);
                        // }
                    } else {
                        return null;
                    }
                } else {
                    return getTransformedShape(RendererUtilities.getCentroid(g), sa);
                }
            } else {
                return getTransformedShape(g, sa);
            }
        } catch (TransformException te) {
            LOGGER.trace(te.getLocalizedMessage(), te);
            // fireErrorEvent(te);
            return null;
        } catch (AssertionError ae) {
            LOGGER.trace(ae.getLocalizedMessage(), ae);
            // fireErrorEvent(ae);
            return null;
        }
    }

    private int getGeometryIndex(Geometry g) {
        for (int i = 0; i < geometries.size(); i++) {
            if (geometries.get(i) == g) {
                return i;
            }
        }
        return -1;
    }

    @Nullable
    private LiteShape2 getTransformedShape(final Geometry originalGeom,
            final SymbolizerAssociation sa) throws TransformException, FactoryException {
        {
            int idx = getGeometryIndex(originalGeom);
            if (idx > -1) {
                return (LiteShape2) shapes.get(idx);
            }
        }

        // we need to clone if the clone flag is high or if the coordinate sequence is not the
        // one we asked for
        Geometry geom = originalGeom;
        if (geom == null) {
            return null;
        }
        if (!(geom.getFactory().getCoordinateSequenceFactory() instanceof LiteCoordinateSequenceFactory)) {
            int dim = sa.crs != null ? sa.crs.getCoordinateSystem().getDimension() : 2;
            geom = LiteCoordinateSequence.cloneGeometry(geom, dim);
        }

        LiteShape2 shape;
        if (sa == null) {
            MathTransform xform = null;
            shape = new LiteShape2(geom, xform, getDecimator(xform), false, false);
        } else {
            // first generalize and transform the geometry into the rendering CRS
            Decimator d = getDecimator(sa.xform);
            geom = d.decimateTransformGeneralize(geom, sa.rxform);
            geom.geometryChanged();
            shape = new LiteShape2(geom, null, null, false, false);
        }

        // cache the result
        geometries.add(originalGeom);
        shapes.add(shape);
        return shape;
    }

    /**
     * @throws org.opengis.referencing.operation.NoninvertibleTransformException
     */
    private Decimator getDecimator(MathTransform mathTransform) {
        // returns a decimator that does nothing if the currently set generalization
        // distance is zero (no generalization desired) or if the datastore has
        // already done full generalization at the desired level
        if (generalizationDistance == 0 || !inMemoryGeneralization) {
            return NULL_DECIMATOR;
        }

        Decimator decimator = (Decimator) decimators.get(mathTransform);
        if (decimator == null) {
            try {
                if (mathTransform != null && !mathTransform.isIdentity())
                    decimator = new Decimator(mathTransform.inverse(), screenSize,
                            generalizationDistance);
                else
                    decimator = new Decimator(null, screenSize, generalizationDistance);
            } catch (org.opengis.referencing.operation.NoninvertibleTransformException e) {
                decimator = new Decimator(null, screenSize, generalizationDistance);
            }

            decimators.put(mathTransform, decimator);
        }
        return decimator;
    }

    private Geometry findGeometry(final Object drawMe, final Symbolizer s) {
        if (drawMe instanceof Geometry) {
            return (Geometry) drawMe;
        }
        Expression geomExpr = s.getGeometry();

        // get the geometry
        Geometry geom;
        if (geomExpr == null) {
            if (drawMe instanceof SimpleFeature) {
                geom = (Geometry) ((SimpleFeature) drawMe).getDefaultGeometry();
            } else if (drawMe instanceof Feature) {
                geom = (Geometry) ((Feature) drawMe).getDefaultGeometryProperty().getValue();
            } else {
                geom = defaultGeometryPropertyName.evaluate(drawMe, Geometry.class);
            }
        } else {
            geom = geomExpr.evaluate(drawMe, Geometry.class);
        }

        return geom;
    }

    private CoordinateReferenceSystem findGeometryCS(Object drawMe, Symbolizer s) {
        if (drawMe instanceof Geometry) {
            Geometry g = (Geometry) drawMe;
            if (g.getUserData() instanceof CoordinateReferenceSystem) {
                return (CoordinateReferenceSystem) g.getUserData();
            }
            return this.defaultNativeCrs;
        }
        if (drawMe instanceof Feature) {
            Feature f = (Feature) drawMe;
            FeatureType schema = f.getType();

            Expression geometry = s.getGeometry();

            if (geometry instanceof PropertyName) {
                return getAttributeCRS((PropertyName) geometry, schema);
            } else if (geometry == null) {
                return getAttributeCRS(null, schema);
            } else {
                StyleAttributeExtractor attExtractor = new StyleAttributeExtractor();
                geometry.accept(attExtractor, null);
                for (PropertyName name : attExtractor.getAttributes()) {
                    if (name.evaluate(schema) instanceof GeometryDescriptor) {
                        return getAttributeCRS(name, schema);
                    }
                }
            }
        }

        return this.defaultNativeCrs;
    }

    /**
     * Finds the CRS of the specified attribute (or uses the default geometry instead)
     * 
     * @param geomName
     * @param schema
     * @return
     */
    CoordinateReferenceSystem getAttributeCRS(PropertyName geomName, FeatureType schema) {
        if (geomName == null || "".equals(geomName.getPropertyName())) {
            GeometryDescriptor geom = schema.getGeometryDescriptor();
            return geom.getType().getCoordinateReferenceSystem();
        } else {
            GeometryDescriptor geom = (GeometryDescriptor) geomName.evaluate(schema);
            return geom.getType().getCoordinateReferenceSystem();
        }
    }

    public Object content() {
        return content;
    }

    public void setScreenMap(ScreenMap screenMap) {
        this.screenMap = screenMap;
    }
}
