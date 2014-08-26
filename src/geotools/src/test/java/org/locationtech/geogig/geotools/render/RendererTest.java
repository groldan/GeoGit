package org.locationtech.geogig.geotools.render;

import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.geotools.data.FeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.map.FeatureLayer;
import org.geotools.map.Layer;
import org.geotools.map.MapContent;
import org.geotools.renderer.lite.StreamingRenderer;
import org.geotools.styling.SLDParser;
import org.geotools.styling.Style;
import org.geotools.styling.StyleFactoryImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geogig.api.porcelain.CommitOp;
import org.locationtech.geogig.geotools.data.GeoGigDataStore;
import org.locationtech.geogig.test.integration.RepositoryTestCase;

public class RendererTest extends RepositoryTestCase {

    private static Style point, line, polygon;

    @Override
    protected void setUpInternal() throws Exception {
        insertAndAdd(points1, points2, points3);
        geogig.command(CommitOp.class).setMessage("points").call();
        insertAndAdd(lines1, lines2, lines3);
        geogig.command(CommitOp.class).setMessage("lines").call();
    }

    @BeforeClass
    public static void classSetup() {
        SLDParser p = new SLDParser(new StyleFactoryImpl());
        p.setInput(RendererTest.class.getResourceAsStream("point.sld"));
        p.parseSLD();
        point = p.readDOM()[0];

        p.setInput(RendererTest.class.getResourceAsStream("line.sld"));
        p.parseSLD();
        line = p.readDOM()[0];

        p.setInput(RendererTest.class.getResourceAsStream("polygon.sld"));
        p.parseSLD();
        polygon = p.readDOM()[0];
    }

    @Test
    public void test() throws IOException {
        BufferedImage img = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB);
        Graphics2D graphics = img.createGraphics();
        Rectangle paintArea = new Rectangle(img.getWidth(), img.getHeight());

        ReferencedEnvelope mapArea = super.boundsOf(points1, points2, points3);
        mapArea.expandBy(10, 10);
        Renderer renderer = new Renderer(geogig);

        renderer.addLayer(pointsName, point);
        renderer.paint(graphics, paintArea, mapArea);

        graphics.dispose();
        ImageIO.write(img, "PNG", new File("image.png"));

        MapContent map = new MapContent();
        FeatureSource featureSource = new GeoGigDataStore(geogig).getFeatureSource(pointsName);
        Layer layer = new FeatureLayer(featureSource, point);
        map.addLayer(layer);

        img = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB);
        graphics = img.createGraphics();
        StreamingRenderer r = new StreamingRenderer();
        r.setMapContent(map);
        r.paint(graphics, paintArea, mapArea);
        graphics.dispose();
        ImageIO.write(img, "PNG", new File("image-gt.png"));
    }

}
