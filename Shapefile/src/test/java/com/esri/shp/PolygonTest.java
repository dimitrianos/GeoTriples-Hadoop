package com.esri.shp;

//import com.esri.core.geometry.Envelope2D;
//import com.esri.core.geometry.Polygon;


import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import org.junit.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 */
public class PolygonTest
{
    @Test
    public void testReadPolygon() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpolygon.shp");
        assertNotNull(inputStream);
        try
        {
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertTrue(shpReader.hasMore());
            
            final Polygon polygon = shpReader.readPolygon();
            
            final Geometry gf = polygon.getEnvelope();             
            
            final Envelope enveloper2D = gf.getEnvelopeInternal();        

            assertEquals(shpHeader.xmin, enveloper2D.getMinX(), 0.000001);
            assertEquals(shpHeader.ymin, enveloper2D.getMinY(), 0.000001);
            assertEquals(shpHeader.xmax, enveloper2D.getMaxX(), 0.000001);
            assertEquals(shpHeader.ymax, enveloper2D.getMaxY(), 0.000001);
            
        }
        finally
        {
            inputStream.close();
        }
    }

    @Test
    public void testQueryPolygon() throws IOException
    {
        final InputStream inputStream = this.getClass().getResourceAsStream("/testpolygon.shp");
        assertNotNull(inputStream);
        try
        {
            final ShpReader shpReader = new ShpReader(new DataInputStream(inputStream));
            final ShpHeader shpHeader = shpReader.getHeader();
            assertEquals(5, shpHeader.shapeType);
            assertTrue(shpReader.hasMore());
     
            
            Polygon polygon;            
            GeometryFactory gf=new GeometryFactory();
        
            Coordinate[] coords  =
            new Coordinate[] {new Coordinate(0, 0), new Coordinate(10, 0),
                          new Coordinate(10, 10),  new Coordinate(0,0) };

            LinearRing ring = gf.createLinearRing( coords );
            LinearRing holes[] = null;
            polygon = gf.createPolygon(ring, holes);
            
            polygon = shpReader.queryPolygon(polygon);
            
            final Geometry gm = polygon.getEnvelope();             
            
            final Envelope enveloper2D = gm.getEnvelopeInternal();
                       
            assertEquals(shpHeader.xmin, enveloper2D.getMinX(), 0.000001);
            assertEquals(shpHeader.ymin, enveloper2D.getMinY(), 0.000001);
            assertEquals(shpHeader.xmax, enveloper2D.getMaxX(), 0.000001);
            assertEquals(shpHeader.ymax, enveloper2D.getMaxY(), 0.000001);
            
        }
        finally
        {
            inputStream.close();
        }
    }
}
