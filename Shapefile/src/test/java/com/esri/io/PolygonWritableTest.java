package com.esri.io;

import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 */
public class PolygonWritableTest
{
    @Test
    public void testWriteRead() throws IOException
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        final PolygonWritable polygonWritable = new PolygonWritable();
        polygonWritable.readFields(dataInput);

        final Polygon polygon = polygonWritable.polygon;
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

    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PolygonWritable polygonWritable = new PolygonWritable();
      

                GeometryFactory gf=new GeometryFactory();
        
        Coordinate[] coords  =
                   new Coordinate[] {new Coordinate(-104.84461178011469, 39.25684342186668), new Coordinate(-111.19319480258162, 37.038663811607194),
                          new Coordinate(-118.45964524998351, 38.56844285316538),  new Coordinate(-117.46528887297058,43.00480207368446), 
                          new Coordinate(-110.27532737764665, 44.76404797147654), new Coordinate(-104.84461178011469, 39.25684342186668)};

        LinearRing ring = gf.createLinearRing( coords );
        LinearRing holes[] = null;        
     
        polygonWritable.polygon = gf.createPolygon(ring, holes);     
        
       
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        polygonWritable.write(dataOutput);

        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
