package com.esri.io;




import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 */
public class PolygonFeatureWritableTest
{
    @Test
    public void testWriteRead() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final PolygonFeatureWritable polygonFeatureWritable = new PolygonFeatureWritable();
        polygonFeatureWritable.readFields(new DataInputStream(byteArrayInputStream));

        final Polygon polygon = polygonFeatureWritable.polygon;
        final Coordinate[] coordinates2D = polygon.getCoordinates();
   
        assertEquals(6, coordinates2D.length);

        assertEquals(-104.84461178011469, coordinates2D[0].x, 0.000001);
        assertEquals(39.25684342186668, coordinates2D[0].y, 0.000001);

        assertEquals(-111.19319480258162, coordinates2D[1].x, 0.000001);
        assertEquals(37.038663811607194, coordinates2D[1].y, 0.000001);

        assertEquals(-118.45964524998351, coordinates2D[2].x, 0.000001);
        assertEquals(38.56844285316538, coordinates2D[2].y, 0.000001);

        assertEquals(-117.46528887297058, coordinates2D[3].x, 0.000001);
        assertEquals(43.00480207368446, coordinates2D[3].y, 0.000001);
        
        assertEquals(-110.27532737764665, coordinates2D[4].x, 0.000001);
        assertEquals(44.76404797147654, coordinates2D[4].y, 0.000001);
        
        assertEquals(-104.84461178011469, coordinates2D[5].x, 0.000001);
        assertEquals(39.25684342186668, coordinates2D[5].y, 0.000001);

        assertEquals(new LongWritable(1234), polygonFeatureWritable.attributes.get(new Text("key")));
    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PolygonFeatureWritable pointFeatureWritable = new PolygonFeatureWritable();
   
        
        GeometryFactory gf=new GeometryFactory();
        
        Coordinate[] coords  =
        new Coordinate[] {new Coordinate(-104.84461178011469, 39.25684342186668), new Coordinate(-111.19319480258162, 37.038663811607194),
                          new Coordinate(-118.45964524998351, 38.56844285316538),  new Coordinate(-117.46528887297058,43.00480207368446), 
                          new Coordinate(-110.27532737764665, 44.76404797147654), new Coordinate(-104.84461178011469, 39.25684342186668)};
        
        LinearRing ring = gf.createLinearRing( coords );
        LinearRing holes[] = null;
            
        pointFeatureWritable.polygon = gf.createPolygon(ring, holes);        
                  
        pointFeatureWritable.attributes.put(new Text("key"), new LongWritable(1234));

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        pointFeatureWritable.write(new DataOutputStream(byteArrayOutputStream));
        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
