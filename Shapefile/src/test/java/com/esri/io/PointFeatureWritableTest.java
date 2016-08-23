package com.esri.io;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
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
public class PointFeatureWritableTest
{
    @Test
    public void testWriteRead() throws Exception
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        final PointFeatureWritable pointFeatureWritable = new PointFeatureWritable();
        pointFeatureWritable.readFields(new DataInputStream(byteArrayInputStream));

        assertEquals(10.0, pointFeatureWritable.point.getX(), 0.000001);
        assertEquals(11.0, pointFeatureWritable.point.getY(), 0.000001);
        assertEquals(new LongWritable(1234), pointFeatureWritable.attributes.get(new Text("key")));
    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PointFeatureWritable pointFeatureWritable = new PointFeatureWritable();
        
        GeometryFactory gf = new GeometryFactory();

        Coordinate coord = new Coordinate( 10, 11 );
        Point point = gf.createPoint( coord );    
                
        pointFeatureWritable.point = point;
        pointFeatureWritable.attributes.put(new Text("key"), new LongWritable(1234));

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        pointFeatureWritable.write(new DataOutputStream(byteArrayOutputStream));
        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }

}
