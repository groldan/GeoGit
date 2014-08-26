package org.locationtech.geogig.geotools.render;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.collect.Iterators.filter;
import static org.locationtech.geogig.geotools.render.RenderSupport.buildMapTransform;
import static org.locationtech.geogig.geotools.render.RenderSupport.computeRenderingBuffer;
import static org.locationtech.geogig.geotools.render.RenderSupport.computeScale;
import static org.locationtech.geogig.geotools.render.RenderSupport.expandEnvelope;
import static org.locationtech.geogig.geotools.render.RenderSupport.resolveFeature;
import static org.locationtech.geogig.geotools.render.RenderSupport.worldToScreenTransform;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.RenderingHints;
import java.awt.Transparency;
import java.awt.geom.AffineTransform;
import java.awt.geom.NoninvertibleTransformException;
import java.awt.geom.Point2D;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.media.jai.Interpolation;

import org.geotools.coverage.grid.GridEnvelope2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.factory.Hints;
import org.geotools.filter.IllegalFilterException;
import org.geotools.filter.spatial.DefaultCRSFilterVisitor;
import org.geotools.filter.spatial.ReprojectingFilterVisitor;
import org.geotools.filter.visitor.DefaultFilterVisitor;
import org.geotools.filter.visitor.SimplifyingFilterVisitor;
import org.geotools.filter.visitor.SpatialFilterVisitor;
import org.geotools.geometry.jts.Decimator;
import org.geotools.geometry.jts.GeometryClipper;
import org.geotools.geometry.jts.LiteCoordinateSequenceFactory;
import org.geotools.geometry.jts.LiteShape2;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultEngineeringCRS;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.renderer.ScreenMap;
import org.geotools.renderer.lite.GraphicsAwareDpiRescaleStyleVisitor;
import org.geotools.renderer.lite.OpacityFinder;
import org.geotools.renderer.lite.RendererUtilities;
import org.geotools.renderer.lite.StyledShapePainter;
import org.geotools.renderer.style.SLDStyleFactory;
import org.geotools.renderer.style.Style2D;
import org.geotools.styling.FeatureTypeStyle;
import org.geotools.styling.PointSymbolizer;
import org.geotools.styling.Rule;
import org.geotools.styling.RuleImpl;
import org.geotools.styling.Style;
import org.geotools.styling.Symbolizer;
import org.geotools.styling.TextSymbolizer;
import org.geotools.styling.visitor.DpiRescaleStyleVisitor;
import org.geotools.styling.visitor.DuplicatingStyleVisitor;
import org.geotools.styling.visitor.UomRescaleStyleVisitor;
import org.geotools.util.NumberRange;
import org.locationtech.geogig.api.Bucket;
import org.locationtech.geogig.api.Context;
import org.locationtech.geogig.api.FeatureBuilder;
import org.locationtech.geogig.api.GeoGIG;
import org.locationtech.geogig.api.Node;
import org.locationtech.geogig.api.NodeRef;
import org.locationtech.geogig.api.ObjectId;
import org.locationtech.geogig.api.Ref;
import org.locationtech.geogig.api.RevCommit;
import org.locationtech.geogig.api.RevFeatureType;
import org.locationtech.geogig.api.RevTree;
import org.locationtech.geogig.api.plumbing.DiffTree;
import org.locationtech.geogig.api.plumbing.FindTreeChild;
import org.locationtech.geogig.api.plumbing.RevObjectParse;
import org.locationtech.geogig.api.plumbing.diff.DiffTreeVisitor.Consumer;
import org.locationtech.geogig.storage.ObjectDatabase;
import org.opengis.feature.Feature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.FeatureType;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.Name;
import org.opengis.feature.type.PropertyDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.filter.Id;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.identity.FeatureId;
import org.opengis.filter.identity.Identifier;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.crs.SingleCRS;
import org.opengis.referencing.datum.PixelInCell;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.opengis.style.LineSymbolizer;
import org.opengis.style.PolygonSymbolizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

/**
 * @TODO: <ul>
 *        <li>Rendering buffer
 *        <li>labels
 *        <li>advanced projection handling/continuous map wrapping
 *        <li>stop rendering on user request
 *        </ul>
 *
 */
public class Renderer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Renderer.class);

    /** Tolerance used to compare doubles for equality */
    private static final double TOLERANCE = 1e-6;

    private static final SLDStyleFactory styleFactory = new SLDStyleFactory();

    /** Filter factory for creating bounding box filters */
    private final static FilterFactory2 filterFactory = CommonFactoryFinder.getFilterFactory2(null);

    private final GeoGIG geogig;

    private Rectangle paintArea;

    /** Geographic map extent, eventually expanded to consider buffer area around the map */
    private ReferencedEnvelope mapArea;

    /** Geographic map extent, as provided by the caller */
    private ReferencedEnvelope originalMapExtent;

    private AffineTransform worldToScreen;

    private LinkedHashMap<String, Style> treeStyles;

    private StyledShapePainter painter = new StyledShapePainter();

    private double scaleDenominator;

    private int renderingBuffer;

    private RenderingHints java2dHints;

    private boolean renderingStopRequested;

    public Renderer(GeoGIG geogig) {
        checkNotNull(geogig, "geogig");
        this.geogig = geogig;
        this.treeStyles = new LinkedHashMap<>();
    }

    public void setRenderingBuffer(int pixels) {
        this.renderingBuffer = pixels;
    }

    public void paint(Graphics2D graphics, Rectangle paintArea, ReferencedEnvelope mapArea) {
        checkNotNull(graphics, "graphics");
        checkNotNull(paintArea, "paintArea");
        checkNotNull(mapArea, "mapArea");
        this.paintArea = paintArea;
        this.mapArea = mapArea;
        this.originalMapExtent = new ReferencedEnvelope(mapArea);
        this.worldToScreen = worldToScreenTransform(mapArea, paintArea);
        checkState(worldToScreen != null, "Unable to find a world to screen transformation");

        this.scaleDenominator = computeScale(mapArea, paintArea, worldToScreen, (Hints) null);
        // add the anchor for graphic fills
        Point2D textureAnchor = new Point2D.Double(worldToScreen.getTranslateX(),
                worldToScreen.getTranslateY());
        graphics.setRenderingHint(StyledShapePainter.TEXTURE_ANCHOR_HINT_KEY, textureAnchor);

        graphics.setClip(paintArea);
        graphics.setTransform(worldToScreen);

        for (Map.Entry<String, Style> layer : treeStyles.entrySet()) {
            final String treeish = layer.getKey();
            final Style style = layer.getValue();
            processStylers(graphics, treeish, style);
        }
    }

    private void expandMapAreaAndScreenMap(final int renderingBuffer,
            List<LiteFeatureTypeStyle> styles) {
        mapArea = expandEnvelope(mapArea, worldToScreen, renderingBuffer);
        LOGGER.trace("Expanding rendering area by {} pixels to consider stroke width",
                renderingBuffer);

        // expand the screenmaps by the meta buffer, otherwise we'll throw away geomtries
        // that sit outside of the map, but whose symbolizer may contribute to it
        for (LiteFeatureTypeStyle lfts : styles) {
            if (lfts.screenMap != null) {
                lfts.screenMap = new ScreenMap(lfts.screenMap, renderingBuffer);
            }
        }
    }

    public void addLayer(final String treeIsh, final Style style) {
        checkNotNull(treeIsh, "tree-ish ref spec not provided");
        // checkNotNull(style, "style for %s not provided", treeIsh);
        // Optional<ObjectId> treeId =
        // geogig.command(ResolveTreeish.class).setTreeish(treeIsh).call();
        // checkArgument(treeId.isPresent(), "Ref spec '%s' did not resolve to a tree", treeId);

        treeStyles.put(treeIsh, style);
    }

    private class RenderingConsumer implements Consumer {

        private final boolean renderLeft;

        private final boolean renderRight;

        private final RenderableFeature rf;

        private final NumberRange<Double> scaleRange;

        private final LiteFeatureTypeStyle liteFeatureTypeStyle;

        private final ObjectDatabase odb = geogig.getContext().objectDatabase();

        private FeatureBuilder builder;

        public RenderingConsumer(FeatureType schema, RenderableFeature rf,
                NumberRange<Double> scaleRange, LiteFeatureTypeStyle liteFeatureTypeStyle,
                boolean renderLeft, boolean renderRight) {
            this.rf = rf;
            this.scaleRange = scaleRange;
            this.liteFeatureTypeStyle = liteFeatureTypeStyle;
            this.renderLeft = renderLeft;
            this.renderRight = renderRight;
            this.builder = new FeatureBuilder((SimpleFeatureType) schema);
        }

        @Override
        public void feature(Node left, Node right) {
            try {
                if (renderLeft && left != null) {
                    render(left);
                }
                if (renderRight && right != null) {
                    render(right);
                }
            } catch (Exception e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public boolean tree(Node left, Node right) {
            return true;
        }

        @Override
        public void endTree(Node left, Node right) {
            // nothing to do
        }

        @Override
        public boolean bucket(int bucketIndex, int bucketDepth, Bucket left, Bucket right) {
            return true;
        }

        @Override
        public void endBucket(int bucketIndex, int bucketDepth, Bucket left, Bucket right) {
            // nothing to do
        }

        private void render(final Node node) throws Exception {
            Feature feature = resolveFeature(node, builder, odb);
            CoordinateReferenceSystem destCRS = mapArea.getCoordinateReferenceSystem();
            CoordinateReferenceSystem nativeCrs = feature.getType().getCoordinateReferenceSystem();
            rf.setFeature(feature, nativeCrs);
            // draw the feature on the main graphics and on the eventual extra image
            // buffers
            process(rf, liteFeatureTypeStyle, scaleRange, worldToScreen, destCRS);
        }
    }

    private void processStylers(final Graphics2D graphics, final String treeish, Style style) {
        final String[] commitAndTree = treeish.split(":");

        checkState(commitAndTree.length > 0 && commitAndTree.length < 3);

        final String commitish = commitAndTree.length == 1 ? Ref.HEAD : commitAndTree[0];
        final String treePath = commitAndTree.length == 1 ? commitAndTree[0] : commitAndTree[1];
        Optional<RevCommit> commit = geogig.command(RevObjectParse.class).setRefSpec(commitish)
                .call(RevCommit.class);
        Optional<RevTree> root = geogig.command(RevObjectParse.class)
                .setObjectId(commit.get().getTreeId()).call(RevTree.class);
        Optional<NodeRef> treeRef = geogig.command(FindTreeChild.class).setParent(root.get())
                .setChildPath(treePath).call();

        checkState(treeRef.isPresent(), "Tree not found: %s", treeish);

        final NodeRef layerRef = treeRef.get();

        final CoordinateReferenceSystem destinationCrs = this.mapArea
                .getCoordinateReferenceSystem();
        /*
         * DJB: changed this a wee bit so that it now does the layer query AFTER it has evaluated
         * the rules for scale inclusion. This makes it so that geometry columns (and other columns)
         * will not be queried unless they are actually going to be required. see geos-469
         */
        // /////////////////////////////////////////////////////////////////////
        //
        // Preparing feature information and styles
        //
        // /////////////////////////////////////////////////////////////////////

        final CoordinateReferenceSystem sourceCrs;
        final NumberRange<Double> scaleRange = NumberRange.create(scaleDenominator,
                scaleDenominator);

        final FeatureType schema = getSchema(layerRef.getMetadataId());

        final GeometryDescriptor geometryAttribute = schema.getGeometryDescriptor();
        if (geometryAttribute != null && geometryAttribute.getType() != null) {
            sourceCrs = geometryAttribute.getType().getCoordinateReferenceSystem();
        } else {
            sourceCrs = null;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Processing {} stylers for {}", style.featureTypeStyles().size(), treeish);
        }

        final ArrayList<LiteFeatureTypeStyle> lfts;
        lfts = createLiteFeatureTypeStyles(style.featureTypeStyles(), schema, graphics);
        if (lfts.isEmpty()) {
            return;
        }
        this.renderingBuffer = findRenderingBuffer(lfts);
        if (this.renderingBuffer > 0) {
            expandMapAreaAndScreenMap(renderingBuffer, lfts);
        }

        // make sure all spatial filters in the feature source native SRS
        reprojectSpatialFilters(lfts, schema);

        // apply the uom and dpi rescale
        applyUnitRescale(lfts);

        // classify by transformation
        final List<List<LiteFeatureTypeStyle>> txClassified = classifyByTransformation(lfts);

        // render groups by uniform transformation
        for (List<LiteFeatureTypeStyle> uniform : txClassified) {
            // ... assume we have to do the generalization, the query layer process will
            // turn down the flag if we don't
            // boolean inMemoryGeneralization = true;
            Query styleQuery = getStyleQuery(schema, uniform, sourceCrs, geometryAttribute);

            // finally, perform rendering
            if (isOptimizedFTSRenderingEnabled() && lfts.size() > 0) {
                drawOptimized(graphics, commitish, layerRef, schema, destinationCrs, scaleRange,
                        uniform, styleQuery);
            } else {
                // drawPlain(graphics, commitish, layerRef, destinationCrs, scaleRange, uniform,
                // styleQuery);
            }
        }
    }

    private boolean isOptimizedFTSRenderingEnabled() {
        return true;
    }

    /**
     * Performs all rendering on the user provided graphics object by scanning the collection
     * multiple times, one for each feature type style provided
     * 
     * @param styleQuery
     */
    private void drawPlain(final Graphics2D graphics, final NodeRef currLayer,
            final CoordinateReferenceSystem destinationCrs, final NumberRange<?> scaleRange,
            final List<LiteFeatureTypeStyle> lfts, Query styleQuery) {
        //
        // final AffineTransform at = worldToScreen;
        // final LiteFeatureTypeStyle[] fts_array = lfts
        // .toArray(new LiteFeatureTypeStyle[lfts.size()]);
        //
        // // for each lite feature type style, scan the whole collection and draw
        // for (LiteFeatureTypeStyle liteFeatureTypeStyle : fts_array) {
        // Iterator<?> iterator = null;
        // if (collection != null) {
        // iterator = collection.iterator();
        // if (iterator == null) {
        // return; // nothing to do
        // }
        // } else if (features != null) {
        // FeatureIterator<?> featureIterator = ((FeatureCollection<?, ?>) features)
        // .features();
        // if (featureIterator == null) {
        // return; // nothing to do
        // }
        // iterator = DataUtilities.iterator(featureIterator);
        // } else {
        // return; // nothing to do
        // }
        // try {
        // ScreenMap screenMap = liteFeatureTypeStyle.screenMap;
        // RenderableFeature rf = new RenderableFeature(screenMap, worldToScreen,
        // destinationCrs, paintArea);
        // // loop exit condition tested inside try catch
        // // make sure we test hasNext() outside of the try/cath that follows, as that
        // // one is there to make sure a single feature error does not ruin the rendering
        // // (best effort) whilst an exception in hasNext() + ignoring catch results in
        // // an infinite loop
        // while (iterator.hasNext() && !renderingStopRequested) {
        // try {
        // rf.setFeature(iterator.next());
        // process(rf, liteFeatureTypeStyle, scaleRange, at, destinationCrs, layerId);
        // } catch (Throwable tr) {
        // fireErrorEvent(tr);
        // }
        // }
        // } finally {
        // DataUtilities.close(iterator);
        // }
        // }
    }

    /**
     * Performs rendering so that the collection is scanned only once even in presence of multiple
     * feature type styles, using the in memory buffer for each feature type style other than the
     * first one (that uses the graphics provided by the user)s
     * 
     * @param commitish
     * @param schema
     * 
     * @param styleQuery
     */
    private void drawOptimized(final Graphics2D graphics, final String commitish,
            NodeRef currLayer, FeatureType schema, CoordinateReferenceSystem destinationCrs,
            final NumberRange<Double> scaleRange, final List<LiteFeatureTypeStyle> lfts,
            Query styleQuery) {

        RenderableFeature rf = new RenderableFeature(worldToScreen, destinationCrs, paintArea);

        final Filter filter = styleQuery.getFilter();
        final ReferencedEnvelope queryBounds = getQueryBounds(schema, filter);
        final Context context = geogig.getContext();

        DiffTree diffOp = context.command(DiffTree.class);
        diffOp.setOldVersion(RevTree.EMPTY_TREE_ID.toString());
        diffOp.setNewVersion(commitish);

        final List<String> pathFilters = resolvePathFilters(currLayer.path(), filter);
        diffOp.setPathFilter(pathFilters);
        if (!queryBounds.isEmpty()) {
            diffOp.setBoundsFilter(queryBounds);
        }

        // diffOp.setChangeTypeFilter(changeType(changeType));

        ScreenMap screenMap;
        for (LiteFeatureTypeStyle style : lfts) {
            ScreenMapFilter screenMapFilter = null;
            screenMap = style.screenMap;
            if (screenMap != null) {
                LOGGER.trace("Using screenMapFilter");
                rf.setScreenMap(screenMap);
                screenMapFilter = new ScreenMapFilter(screenMap);
                diffOp.setCustomFilter(screenMapFilter);
            } else {
                LOGGER.trace("screenMapFilter not available");
            }
            RenderingConsumer consumer = new RenderingConsumer(schema, rf, scaleRange, style, true,
                    true);
            diffOp.call(consumer);
            if (screenMapFilter != null) {
                System.err.printf("Filter result for %s: %s\n", currLayer, screenMapFilter.stats());
            }
        }

        // for each feature:
        // Object feature = null;
        // CoordinateReferenceSystem nativeCrs = null;
        // rf.setFeature(feature, nativeCrs);
        // // draw the feature on the main graphics and on the eventual extra image
        // // buffers
        // for (LiteFeatureTypeStyle liteFeatureTypeStyle : fts_array) {
        // rf.setScreenMap(liteFeatureTypeStyle.screenMap);
        // process(rf, liteFeatureTypeStyle, scaleRange, at, destinationCrs);
        // }

    }

    private ReferencedEnvelope getQueryBounds(FeatureType schema, Filter filterInNativeCrs) {
        CoordinateReferenceSystem crs = schema.getCoordinateReferenceSystem();
        if (crs == null) {
            crs = DefaultEngineeringCRS.GENERIC_2D;
        }
        ReferencedEnvelope queryBounds = new ReferencedEnvelope(crs);
        Envelope bounds = (Envelope) filterInNativeCrs.accept(new ExtractBounds(), queryBounds);
        if (bounds != null) {
            queryBounds.expandToInclude(bounds);
        }
        return queryBounds;
    }

    private static class ExtractBounds extends DefaultFilterVisitor {

        @Override
        public @Nullable Envelope visit(Literal literal, @Nullable Object data) {

            Envelope env = (Envelope) data;
            Object value = literal.getValue();
            if (value instanceof Geometry) {
                if (env == null) {
                    env = new Envelope();
                }
                Envelope literalEnvelope = ((Geometry) value).getEnvelopeInternal();
                env.expandToInclude(literalEnvelope);
            }
            return env;
        }
    }

    private List<String> resolvePathFilters(String typeTreePath, Filter filter) {
        List<String> pathFilters;
        if (filter instanceof Id) {
            final Set<Identifier> identifiers = ((Id) filter).getIdentifiers();
            Iterator<FeatureId> featureIds = filter(
                    filter(identifiers.iterator(), FeatureId.class), notNull());
            Preconditions.checkArgument(featureIds.hasNext(), "Empty Id filter");
            pathFilters = new ArrayList<>();
            while (featureIds.hasNext()) {
                String fid = featureIds.next().getID();
                pathFilters.add(NodeRef.appendChild(typeTreePath, fid));
            }
        } else {
            pathFilters = ImmutableList.of(typeTreePath);
        }
        return pathFilters;
    }

    private void process(RenderableFeature rf, LiteFeatureTypeStyle fts,
            NumberRange<Double> scaleRange, AffineTransform at,
            CoordinateReferenceSystem destinationCrs) throws Exception {

        boolean doElse = true;
        Rule[] elseRuleList = fts.elseRules;
        Rule[] ruleList = fts.ruleList;
        Rule r;
        Filter filter;
        Graphics2D graphics = fts.graphics;
        // applicable rules
        final int length = ruleList.length;
        for (int t = 0; t < length; t++) {
            r = ruleList[t];
            filter = r.getFilter();

            if (filter == null || filter.evaluate(rf.content())) {
                doElse = false;
                processSymbolizers(graphics, rf, r.symbolizers(), scaleRange, at, destinationCrs);
            }
        }

        if (doElse) {
            final int elseLength = elseRuleList.length;
            for (int tt = 0; tt < elseLength; tt++) {
                r = elseRuleList[tt];

                processSymbolizers(graphics, rf, r.symbolizers(), scaleRange, at, destinationCrs);

            }
        }
    }

    /**
     * Applies each of a set of symbolizers in turn to a given feature.
     * <p>
     * This is an internal method and should only be called by processStylers.
     * </p>
     * 
     * @param currLayer
     * 
     * @param graphics
     * @param drawMe The feature to be rendered
     * @param symbolizers An array of symbolizers which actually perform the rendering.
     * @param scaleRange The scale range we are working on... provided in order to make the style
     *        factory happy
     * @param shape
     * @param destinationCrs
     * @throws TransformException
     * @throws FactoryException
     */
    private void processSymbolizers(final Graphics2D graphics, final RenderableFeature drawMe,
            final List<Symbolizer> symbolizers, NumberRange<Double> scaleRange, AffineTransform at,
            CoordinateReferenceSystem destinationCrs) throws Exception {

        for (Symbolizer symbolizer : symbolizers) {
            // /////////////////////////////////////////////////////////////////
            //
            // FEATURE
            //
            // /////////////////////////////////////////////////////////////////
            LiteShape2 shape = drawMe.getShape(symbolizer, at);
            if (shape == null) {
                continue;
            }

            if (symbolizer instanceof TextSymbolizer && drawMe.content() instanceof Feature) {

                return;
            }

            Style2D style = styleFactory.createStyle(drawMe.content(), symbolizer, scaleRange);

            // clip to the visible area + the size of the symbolizer (with some extra
            // to make sure we get no artefacts from polygon new borders)
            double size = RendererUtilities.getStyle2DSize(style);
            // take into account the meta buffer to try and clip all geometries by the same
            // amount
            double clipBuffer = Math.max(size / 2, this.renderingBuffer) + 10;
            Envelope env = new Envelope(paintArea.getMinX(), paintArea.getMaxX(),
                    paintArea.getMinY(), paintArea.getMaxY());
            env.expandBy(clipBuffer);
            final GeometryClipper clipper = new GeometryClipper(env);
            Geometry g = clipper.clip(shape.getGeometry(), false);
            if (g == null) {
                continue;
            }
            if (g != shape.getGeometry()) {
                shape = new LiteShape2(g, null, null, false);
            }

            final boolean isLabelObstacle = false;
            painter.paint(graphics, shape, style, scaleDenominator, isLabelObstacle);
        }
        // only emit a feature drawn event if we actually painted something with it,
        // if it has been clipped out or eliminated by the screenmap we won't emit the event instead
        // if (paintCommands > 0) {
        // requests.put(new FeatureRenderedRequest(drawMe.content));
        // }
    }

    private int findRenderingBuffer(List<LiteFeatureTypeStyle> styles) {
        int renderingBuffer = this.renderingBuffer;
        if (renderingBuffer == 0) {
            renderingBuffer = computeRenderingBuffer(styles);
        }
        return renderingBuffer;
    }

    /**
     * <p>
     * Creates a list of <code>LiteFeatureTypeStyle</code>s with:
     * <ol type="a">
     * <li>out-of-scale rules removed</li>
     * <li>incompatible FeatureTypeStyles removed</li>
     * </ol>
     * </p>
     * 
     * <p>
     * <em><strong>Note:</strong> This method has a lot of duplication with 
     * {@link #createLiteFeatureTypeStyles(FeatureTypeStyle[], SimpleFeatureType, Graphics2D)}. 
     * </em>
     * </p>
     * 
     * @param featureStyles Styles to process
     * @param typeDescription The type description that has to be matched
     * @return ArrayList<LiteFeatureTypeStyle>
     */
    private ArrayList<LiteFeatureTypeStyle> createLiteFeatureTypeStyles(
            List<FeatureTypeStyle> featureStyles, Object typeDescription, Graphics2D graphics) {
        ArrayList<LiteFeatureTypeStyle> result = new ArrayList<LiteFeatureTypeStyle>();

        List<Rule> rules;
        List<Rule> ruleList;
        List<Rule> elseRuleList;
        LiteFeatureTypeStyle lfts;
        BufferedImage image;

        for (FeatureTypeStyle fts : featureStyles) {
            if (typeDescription == null
                    || typeDescription.toString().indexOf(fts.getFeatureTypeName()) == -1)
                continue;

            // get applicable rules at the current scale
            rules = fts.rules();
            ruleList = new ArrayList<Rule>();
            elseRuleList = new ArrayList<Rule>();

            // gather the active rules
            for (Rule r : rules) {
                if (isWithInScale(r)) {
                    if (r.isElseFilter()) {
                        elseRuleList.add(r);
                    } else {
                        ruleList.add(r);
                    }
                }
            }

            // nothing to render, don't do anything!!
            if ((ruleList.isEmpty()) && (elseRuleList.isEmpty())) {
                continue;
            }

            // first fts, we can reuse the graphics directly
            if (result.isEmpty() || !isOptimizedFTSRenderingEnabled()) {
                lfts = new LiteFeatureTypeStyle(graphics, ruleList, elseRuleList,
                        fts.getTransformation());
            } else {
                image = graphics.getDeviceConfiguration().createCompatibleImage(paintArea.width,
                        paintArea.height, Transparency.TRANSLUCENT);
                Graphics2D backBuffer = image.createGraphics();
                backBuffer.setRenderingHints(graphics.getRenderingHints());
                lfts = new LiteFeatureTypeStyle(backBuffer, ruleList, elseRuleList,
                        fts.getTransformation());
            }
            if (screenMapEnabled(lfts)) {
                lfts.screenMap = new ScreenMap(paintArea.x, paintArea.y, paintArea.width,
                        paintArea.height);
            }

            result.add(lfts);
        }

        return result;
    }

    /**
     * Returns true if the ScreenMap optimization can be applied given the current renderer and
     * configuration and the style to be applied
     * 
     * @param lfts
     * @return
     */
    boolean screenMapEnabled(LiteFeatureTypeStyle lfts) {
        // if (generalizationDistance == 0.0) {
        // return false;
        // }

        OpacityFinder finder = new OpacityFinder(new Class[] { PointSymbolizer.class,
                LineSymbolizer.class, PolygonSymbolizer.class });
        for (Rule r : lfts.ruleList) {
            r.accept(finder);
        }
        for (Rule r : lfts.elseRules) {
            r.accept(finder);
        }

        return true;// !finder.hasOpacity;
    }

    /**
     * Checks if a rule can be triggered at the current scale level
     * 
     * @param r The rule
     * @return true if the scale is compatible with the rule settings
     */
    private boolean isWithInScale(Rule r) {
        return ((r.getMinScaleDenominator() - TOLERANCE) <= scaleDenominator)
                && ((r.getMaxScaleDenominator() + TOLERANCE) > scaleDenominator);
    }

    /**
     * Reprojects the spatial filters in each {@link LiteFeatureTypeStyle} so that they match the
     * feature source native coordinate system
     * 
     * @param lfts
     * @param fs
     * @throws FactoryException
     */
    void reprojectSpatialFilters(final ArrayList<LiteFeatureTypeStyle> lfts, FeatureType schema) {
        CoordinateReferenceSystem nativeCRS = getDeclaredSRS(schema);

        // reproject spatial filters in each fts
        for (LiteFeatureTypeStyle fts : lfts) {
            reprojectSpatialFilters(fts, nativeCRS, schema);
        }
    }

    private CoordinateReferenceSystem getDeclaredSRS(FeatureType schema) {
        // compute the default SRS of the feature source
        CoordinateReferenceSystem declaredCRS = schema.getCoordinateReferenceSystem();
        // boolean isEPSGAxisOrderForced = false;
        // if (isEPSGAxisOrderForced) {
        // Integer code = CRS.lookupEpsgCode(declaredCRS, false);
        // if (code != null) {
        // declaredCRS = CRS.decode("urn:ogc:def:crs:EPSG::" + code);
        // }
        // }
        return declaredCRS;
    }

    private FeatureType getSchema(ObjectId metadataId) {
        RevFeatureType featureType = geogig.getContext().stagingDatabase()
                .getFeatureType(metadataId);
        return featureType.type();
    }

    /**
     * Reprojects spatial filters so that they match the feature source native CRS, and assuming all
     * literal geometries are specified in the specified declaredCRS
     */
    void reprojectSpatialFilters(LiteFeatureTypeStyle fts, CoordinateReferenceSystem declaredCRS,
            FeatureType schema) {
        for (int i = 0; i < fts.ruleList.length; i++) {
            fts.ruleList[i] = reprojectSpatialFilters(fts.ruleList[i], declaredCRS, schema);
        }
        if (fts.elseRules != null) {
            for (int i = 0; i < fts.elseRules.length; i++) {
                fts.elseRules[i] = reprojectSpatialFilters(fts.elseRules[i], declaredCRS, schema);
            }
        }
    }

    /**
     * Reprojects spatial filters so that they match the feature source native CRS, and assuming all
     * literal geometries are specified in the specified declaredCRS
     */
    private Rule reprojectSpatialFilters(Rule rule, CoordinateReferenceSystem declaredCRS,
            FeatureType schema) {
        // NPE avoidance
        Filter filter = rule.getFilter();
        if (filter == null) {
            return rule;
        }

        // try to reproject the filter
        Filter reprojected = reprojectSpatialFilter(declaredCRS, filter, schema);
        if (reprojected == filter) {
            return rule;
        }

        // clone the rule (the style can be reused over and over, we cannot alter it) and set the
        // new filter
        Rule rr = new RuleImpl(rule);
        rr.setFilter(reprojected);
        return rr;
    }

    /**
     * Reprojects spatial filters so that they match the feature source native CRS, and assuming all
     * literal geometries are specified in the specified declaredCRS
     */
    private Filter reprojectSpatialFilter(CoordinateReferenceSystem declaredCRS, Filter filter,
            FeatureType schema) {
        // NPE avoidance
        if (filter == null) {
            return null;
        }

        // do we have any spatial filter?
        SpatialFilterVisitor sfv = new SpatialFilterVisitor();
        filter.accept(sfv, null);
        if (!sfv.hasSpatialFilter()) {
            return filter;
        }

        // all right, we need to default the literals to the declaredCRS and then reproject to
        // the native one
        DefaultCRSFilterVisitor defaulter = new DefaultCRSFilterVisitor(filterFactory, declaredCRS);
        Filter defaulted = (Filter) filter.accept(defaulter, null);
        ReprojectingFilterVisitor reprojector = new ReprojectingFilterVisitor(filterFactory, schema);
        Filter reprojected = (Filter) defaulted.accept(reprojector, null);
        return reprojected;
    }

    /**
     * Applies Unit Of Measure rescaling against all symbolizers, the result will be symbolizers
     * that operate purely in pixels
     * 
     * @param lfts
     */
    void applyUnitRescale(final ArrayList<LiteFeatureTypeStyle> lfts) {
        // apply dpi rescale
        double dpi = RendererUtilities.getDpi(getRendererHints());
        double standardDpi = RendererUtilities.getDpi(Collections.emptyMap());
        if (dpi != standardDpi) {
            double scaleFactor = dpi / standardDpi;
            DpiRescaleStyleVisitor dpiVisitor = new GraphicsAwareDpiRescaleStyleVisitor(scaleFactor);
            for (LiteFeatureTypeStyle fts : lfts) {
                rescaleFeatureTypeStyle(fts, dpiVisitor);
            }
        }

        // apply UOM rescaling
        double pixelsPerMeters = RendererUtilities.calculatePixelsPerMeterRatio(scaleDenominator,
                getRendererHints());
        UomRescaleStyleVisitor rescaleVisitor = new UomRescaleStyleVisitor(pixelsPerMeters);
        for (LiteFeatureTypeStyle fts : lfts) {
            rescaleFeatureTypeStyle(fts, rescaleVisitor);
        }
    }

    /**
     * Utility method to apply the two rescale visitors without duplicating code
     * 
     * @param fts
     * @param visitor
     */
    void rescaleFeatureTypeStyle(LiteFeatureTypeStyle fts, DuplicatingStyleVisitor visitor) {
        for (int i = 0; i < fts.ruleList.length; i++) {
            visitor.visit(fts.ruleList[i]);
            fts.ruleList[i] = (Rule) visitor.getCopy();
        }
        if (fts.elseRules != null) {
            for (int i = 0; i < fts.elseRules.length; i++) {
                visitor.visit(fts.elseRules[i]);
                fts.elseRules[i] = (Rule) visitor.getCopy();
            }
        }
    }

    private Map<?, ?> getRendererHints() {
        return null;
    }

    /**
     * Classify a List of LiteFeatureTypeStyle objects by Transformation.
     * 
     * @param lfts A List of LiteFeatureTypeStyles
     * @return A List of List of LiteFeatureTypeStyles
     */
    List<List<LiteFeatureTypeStyle>> classifyByTransformation(List<LiteFeatureTypeStyle> lfts) {
        List<List<LiteFeatureTypeStyle>> txClassified = new ArrayList<List<LiteFeatureTypeStyle>>();
        txClassified.add(new ArrayList<LiteFeatureTypeStyle>());
        Expression transformation = null;
        for (int i = 0; i < lfts.size(); i++) {
            LiteFeatureTypeStyle curr = lfts.get(i);
            if (i == 0) {
                transformation = curr.transformation;
            } else if (!(transformation == curr.transformation)
                    || (transformation != null && curr.transformation != null && !curr.transformation
                            .equals(transformation))) {
                txClassified.add(new ArrayList<LiteFeatureTypeStyle>());

            }
            txClassified.get(txClassified.size() - 1).add(curr);
        }
        return txClassified;
    }

    Query getStyleQuery(FeatureType schema, List<LiteFeatureTypeStyle> styleList,
            CoordinateReferenceSystem featCrs, GeometryDescriptor geometryAttribute) {

        Query query = new Query(Query.ALL);
        Filter filter = null;

        final LiteFeatureTypeStyle[] styles = styleList.toArray(new LiteFeatureTypeStyle[styleList
                .size()]);

        final CoordinateReferenceSystem mapCRS = mapArea.getCoordinateReferenceSystem();
        ReferencedEnvelope envelope = new ReferencedEnvelope(mapArea);

        // see what attributes we really need by exploring the styles
        // for testing purposes we have a null case -->
        // Then create the geometry filters. We have to create one for
        // each geometric attribute used during the rendering as the
        // feature may have more than one and the styles could use non
        // default geometric ones
        List<ReferencedEnvelope> envelopes;
        if (featCrs != null && !CRS.equalsIgnoreMetadata(featCrs, mapCRS)) {
            try {
                envelopes = Collections.singletonList(envelope.transform(featCrs, true, 10));
            } catch (TransformException | FactoryException e) {
                throw Throwables.propagate(e);
            }
        } else {
            envelopes = Collections.singletonList(envelope);
        }

        LOGGER.trace("Querying layer {} with bbox: {}", schema.getName(), envelope);

        filter = createBBoxFilters(schema, envelopes);

        // now build the query using only the attributes and the
        // bounding box needed
        query = new Query(schema.getName().getLocalPart());
        query.setFilter(filter);
        // query.setProperties(attributes);
        // processRuleForQuery(styles, query);

        // prepare hints
        // ... basic one, we want fast and compact coordinate sequences and geometries optimized
        // for the collection of one item case (typical in shapefiles)
        LiteCoordinateSequenceFactory csFactory = new LiteCoordinateSequenceFactory();
        GeometryFactory gFactory = RenderSupport.GF;
        Hints hints = new Hints(Hints.JTS_COORDINATE_SEQUENCE_FACTORY, csFactory);
        hints.put(Hints.JTS_GEOMETRY_FACTORY, gFactory);
        hints.put(Hints.FEATURE_2D, Boolean.TRUE);

        // update the screenmaps
        try {
            CoordinateReferenceSystem crs = featCrs;
            Set<RenderingHints.Key> fsHints = new HashSet<>();
            SingleCRS crs2D = crs == null ? null : CRS.getHorizontalCRS(crs);
            MathTransform mt = buildMapTransform(crs2D, mapCRS, worldToScreen);
            double[] spans = Decimator.computeGeneralizationDistances(mt.inverse(), paintArea,
                    RenderableFeature.generalizationDistance);
            // double distance = spans[0] < spans[1] ? spans[0] : spans[1];
            for (LiteFeatureTypeStyle fts : styles) {
                if (fts.screenMap != null) {
                    fts.screenMap.setTransform(mt);
                    fts.screenMap.setSpans(spans[0], spans[1]);
                    if (fsHints.contains(Hints.SCREENMAP)) {
                        // replace the renderer screenmap with the hint, and avoid doing
                        // the work twice
                        hints.put(Hints.SCREENMAP, fts.screenMap);
                        fts.screenMap = null;
                    }
                }
            }

            // TODO add generalization to be backend
            // if (renderingTransformation) {
            // // the RT might need valid geometries, we can at most apply a topology
            // // preserving generalization
            // if (fsHints.contains(Hints.GEOMETRY_GENERALIZATION)) {
            // hints.put(Hints.GEOMETRY_GENERALIZATION, distance);
            // inMemoryGeneralization = false;
            // }
            // } else {
            // // ... if possible we let the datastore do the generalization
            // if (fsHints.contains(Hints.GEOMETRY_SIMPLIFICATION)) {
            // // good, we don't need to perform in memory generalization, the datastore
            // // does it all for us
            // hints.put(Hints.GEOMETRY_SIMPLIFICATION, distance);
            // inMemoryGeneralization = false;
            // } else if (fsHints.contains(Hints.GEOMETRY_DISTANCE)) {
            // // in this case the datastore can get us close, but we can still
            // // perform some in memory generalization
            // hints.put(Hints.GEOMETRY_DISTANCE, distance);
            // }
            // }
        } catch (Exception e) {
            LOGGER.info("Error computing the generalization hints", e);
        }

        if (query.getHints() == null) {
            query.setHints(hints);
        } else {
            query.getHints().putAll(hints);
        }

        // simplify the filter
        SimplifyingFilterVisitor simplifier = new SimplifyingFilterVisitor();
        Filter simplifiedFilter = (Filter) query.getFilter().accept(simplifier, null);
        query.setFilter(simplifiedFilter);

        return query;
    }

    /**
     * Creates the bounding box filters (one for each geometric attribute) needed to query a
     * <code>MapLayer</code>'s feature source to return just the features for the target rendering
     * extent
     * 
     * @param schema the layer's feature source schema
     * @param attributes set of needed attributes
     * @param bbox the expression holding the target rendering bounding box
     * @return an or'ed list of bbox filters, one for each geometric attribute in
     *         <code>attributes</code>. If there are just one geometric attribute, just returns its
     *         corresponding <code>GeometryFilter</code>.
     * @throws IllegalFilterException if something goes wrong creating the filter
     */
    private Filter createBBoxFilters(FeatureType schema, List<ReferencedEnvelope> bboxes)
            throws IllegalFilterException {
        Filter filter = Filter.INCLUDE;

        for (PropertyDescriptor attType : schema.getDescriptors()) {
            if (attType instanceof GeometryDescriptor) {
                Name geomName = ((GeometryDescriptor) attType).getName();
                Filter gfilter = filterFactory
                        .bbox(filterFactory.property(geomName), bboxes.get(0));

                if (filter == Filter.INCLUDE) {
                    filter = gfilter;
                } else {
                    filter = filterFactory.or(filter, gfilter);
                }

                if (bboxes.size() > 0) {
                    for (int k = 1; k < bboxes.size(); k++) {
                        filter = filterFactory
                                .or(filter,
                                        filterFactory.bbox(filterFactory.property(geomName),
                                                bboxes.get(k)));
                    }
                }
            }
        }

        return filter;
    }

    /**
     * Builds a raster grid geometry that will be used for reading, taking into account the original
     * map extent and target paint area, and expanding the target raster area by
     * {@link #REPROJECTION_RASTER_GUTTER}
     * 
     * @param destinationCrs
     * @param sourceCRS
     * @return
     * @throws NoninvertibleTransformException
     */
    GridGeometry2D getRasterGridGeometry(CoordinateReferenceSystem destinationCrs,
            CoordinateReferenceSystem sourceCRS) throws NoninvertibleTransformException {

        final int REPROJECTION_RASTER_GUTTER = 10;

        GridGeometry2D readGG;
        if (sourceCRS == null || destinationCrs == null
                || CRS.equalsIgnoreMetadata(destinationCrs, sourceCRS)) {
            readGG = new GridGeometry2D(new GridEnvelope2D(paintArea), originalMapExtent);
        } else {
            // reprojection involved, read a bit more pixels to account for rotation
            Rectangle bufferedTargetArea = (Rectangle) paintArea.clone();
            bufferedTargetArea.add( // exand top/right
                    paintArea.x + paintArea.width + REPROJECTION_RASTER_GUTTER, paintArea.y
                            + paintArea.height + REPROJECTION_RASTER_GUTTER);
            bufferedTargetArea.add( // exand bottom/left
                    paintArea.x - REPROJECTION_RASTER_GUTTER, paintArea.y
                            - REPROJECTION_RASTER_GUTTER);

            // now create the final envelope accordingly
            readGG = new GridGeometry2D(new GridEnvelope2D(bufferedTargetArea),
                    PixelInCell.CELL_CORNER, new AffineTransform2D(worldToScreen.createInverse()),
                    originalMapExtent.getCoordinateReferenceSystem(), null);
        }
        return readGG;
    }

    Interpolation getRenderingInterpolation() {
        if (java2dHints == null) {
            return Interpolation.getInstance(Interpolation.INTERP_NEAREST);
        }
        Object interpolationHint = java2dHints.get(RenderingHints.KEY_INTERPOLATION);
        if (interpolationHint == null
                || interpolationHint == RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR) {
            return Interpolation.getInstance(Interpolation.INTERP_NEAREST);
        } else if (interpolationHint == RenderingHints.VALUE_INTERPOLATION_BILINEAR) {
            return Interpolation.getInstance(Interpolation.INTERP_BILINEAR);
        } else {
            return Interpolation.getInstance(Interpolation.INTERP_BICUBIC);
        }
    }

}
