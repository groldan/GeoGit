package org.locationtech.geogig.geotools.render;

import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.util.List;
import java.util.Map;

import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.data.DataUtilities;
import org.geotools.factory.Hints;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.referencing.operation.builder.GridToEnvelopeMapper;
import org.geotools.referencing.operation.matrix.XAffineTransform;
import org.geotools.referencing.operation.transform.ConcatenatedTransform;
import org.geotools.referencing.operation.transform.ProjectiveTransform;
import org.geotools.renderer.lite.MetaBufferEstimator;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.styling.Rule;
import org.locationtech.geogig.api.FeatureBuilder;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.RevFeature;
import org.locationtech.geogig.api.RevObject;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.MathTransform2D;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

class RenderSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(RenderSupport.class);

    static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(
            PrecisionModel.FLOATING_SINGLE), 0, new LiteCoordinateSequenceFactory());

    /**
     * Builds a full transform going from the source CRS to the destination CRS and from there to
     * the screen.
     * <p>
     * Although we ask for 2D content (via {@link Hints#FEATURE_2D} ) not all DataStore
     * implementations are capable. In this event we will manually stage the information into
     * {@link DefaultGeographicCRS#WGS84}) and before using this transform.
     */
    public static MathTransform buildMapTransform(CoordinateReferenceSystem sourceCRS,
            CoordinateReferenceSystem destCRS, AffineTransform worldToScreenTransform)
            throws FactoryException {
        MathTransform mt = buildTransform(sourceCRS, destCRS);

        // concatenate from world to screen
        if (mt != null && !mt.isIdentity()) {
            mt = ConcatenatedTransform.create(mt,
                    ProjectiveTransform.create(worldToScreenTransform));
        } else {
            mt = ProjectiveTransform.create(worldToScreenTransform);
        }

        return mt;
    }

    /**
     * Builds the transform from sourceCRS to destCRS/
     * <p>
     * Although we ask for 2D content (via {@link Hints#FEATURE_2D} ) not all DataStore
     * implementations are capable. With that in mind if the provided soruceCRS is not 2D we are
     * going to manually post-process the Geomtries into {@link DefaultGeographicCRS#WGS84} - and
     * the {@link MathTransform2D} returned here will transition from WGS84 to the requested
     * destCRS.
     * 
     * @param sourceCRS
     * @param destCRS
     * @return the transform, or null if any of the crs is null, or if the the two crs are equal
     * @throws FactoryException If no transform is available to the destCRS
     */
    public static MathTransform buildTransform(CoordinateReferenceSystem sourceCRS,
            CoordinateReferenceSystem destCRS) throws FactoryException {
        MathTransform transform = null;
        if (sourceCRS != null && sourceCRS.getCoordinateSystem().getDimension() >= 3) {
            // We are going to transform over to DefaultGeographic.WGS84 on the fly
            // so we will set up our math transform to take it from there
            MathTransform toWgs84_3d = CRS.findMathTransform(sourceCRS,
                    DefaultGeographicCRS.WGS84_3D);
            MathTransform toWgs84_2d = CRS.findMathTransform(DefaultGeographicCRS.WGS84_3D,
                    DefaultGeographicCRS.WGS84);
            transform = ConcatenatedTransform.create(toWgs84_3d, toWgs84_2d);
            sourceCRS = DefaultGeographicCRS.WGS84;
        }
        // the basic crs transformation, if any
        MathTransform2D mt;
        if (sourceCRS == null || destCRS == null || CRS.equalsIgnoreMetadata(sourceCRS, destCRS)) {
            mt = null;
        } else {
            mt = (MathTransform2D) CRS.findMathTransform(sourceCRS, destCRS, true);
        }

        if (transform != null) {
            if (mt == null) {
                return transform;
            } else {
                return ConcatenatedTransform.create(transform, mt);
            }
        } else {
            return mt;
        }
    }

    public static double computeScale(ReferencedEnvelope envelope, Rectangle paintArea,
            AffineTransform worldToScreen, Map hints) {
        // if(getScaleComputationMethod().equals(SCALE_ACCURATE)) {
        // try {
        // return RendererUtilities.calculateScale(envelope,
        // paintArea.width, paintArea.height, hints);
        // } catch (Exception e) // probably either (1) no CRS (2) error xforming
        // {
        // LOGGER.log(Level.WARNING, e.getLocalizedMessage(), e);
        // }
        // }
        if (XAffineTransform.getRotation(worldToScreen) != 0.0) {
            return RendererUtilities.calculateOGCScaleAffine(
                    envelope.getCoordinateReferenceSystem(), worldToScreen, hints);
        }
        return RendererUtilities.calculateOGCScale(envelope, paintArea.width, hints);
    }

    public static AffineTransform worldToScreenTransform(ReferencedEnvelope mapExtent,
            Rectangle paintArea) {

        // //
        //
        // Convert the JTS envelope and get the transform
        //
        // //
        final Envelope2D genvelope = new Envelope2D(mapExtent);

        // //
        //
        // Get the transform
        //
        // //
        final GridToEnvelopeMapper m = new GridToEnvelopeMapper();
        m.setGridType(PixelInCell.CELL_CORNER);
        try {
            m.setGridRange(new GridEnvelope2D(paintArea));
            m.setEnvelope(genvelope);
            return m.createAffineTransform().createInverse();
        } catch (MismatchedDimensionException e) {
            return null;
        } catch (NoninvertibleTransformException e) {
            return null;
        }
    }

    public static int computeRenderingBuffer(List<LiteFeatureTypeStyle> styles) {

        final MetaBufferEstimator rbe = new MetaBufferEstimator();

        for (LiteFeatureTypeStyle lfts : styles) {
            Rule[] rules = lfts.elseRules;
            for (int j = 0; j < rules.length; j++) {
                rbe.visit(rules[j]);
            }
            rules = lfts.ruleList;
            for (int j = 0; j < rules.length; j++) {
                rbe.visit(rules[j]);
            }
        }
        // if(!rbe.isEstimateAccurate())
        // LOGGER.fine("Assuming rendering buffer = " + rbe.getBuffer()
        // + ", but estimation is not accurate, you may want to set a buffer manually");

        // the actual amount we have to grow the rendering area by is half of the stroke/symbol
        // sizes plus one extra pixel for antialiasing effects
        int renderingBuffer = (int) Math.round(rbe.getBuffer() / 2.0 + 1);

        return renderingBuffer;
    }

    /**
     * Extends the provided {@link Envelope} in order to add the number of pixels specified by
     * <code>buffer</code> in every direction.
     * 
     * @param envelope to extend.
     * @param worldToScreen by means of which doing the extension.
     * @param renderingBufferPixels to use for the extension.
     * @return an extended version of the provided {@link Envelope}.
     */
    public static ReferencedEnvelope expandEnvelope(ReferencedEnvelope envelope,
            AffineTransform worldToScreen, double renderingBufferPixels) {
        assert renderingBufferPixels > 0;
        double bufferX = Math.abs(renderingBufferPixels
                / XAffineTransform.getScaleX0(worldToScreen));
        double bufferY = Math.abs(renderingBufferPixels
                / XAffineTransform.getScaleY0(worldToScreen));

        return new ReferencedEnvelope(envelope.getMinX() - bufferX, envelope.getMaxX() + bufferX,
                envelope.getMinY() - bufferY, envelope.getMaxY() + bufferY,
                envelope.getCoordinateReferenceSystem());
    }

    private static final SimpleFeatureType GEOM_TYPE;
    static {
        try {
            GEOM_TYPE = DataUtilities.createType("default", "geom:Geometry");
        } catch (SchemaException e) {
            e.printStackTrace();
            throw Throwables.propagate(e);
        }
    }

    private static SimpleFeatureBuilder CACHED_GEOM_FEATURE_BUILDER = new SimpleFeatureBuilder(
            GEOM_TYPE);

    public static Feature resolveFeature(final Node node, final FeatureBuilder builder,
            final ObjectDatabase db) {
        Feature f;
        Geometry geom = resolveCachedGeometry(node);
        if (geom == null) {
            RevObject object = db.get(node.getObjectId());
            f = builder.build(node.getName(), (RevFeature) object);
        } else {
            f = CACHED_GEOM_FEATURE_BUILDER.buildFeature(node.getName(), new Object[] { geom });
        }
        return f;
    }

    public static Geometry resolveCachedGeometry(final Node node) {
        Envelope env = new Envelope();
        node.expand(env);
        if (env.isNull()) {
            return null;
        }
        if (env.getWidth() == 0.0 && env.getHeight() == 0.0) {
            return GF.createPoint(new Coordinate(env.getMaxX(), env.getMaxY()));
        }
        return null;
    }

}
