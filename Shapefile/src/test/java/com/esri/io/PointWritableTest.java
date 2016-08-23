package com.esri.io;

//import com.esri.core.geometry.Point;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import org.junit.Test;

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
public class PointWritableTest
{
    @Test
    public void testWriteRead() throws IOException
    {
        final ByteArrayOutputStream byteArrayOutputStream = getByteArrayOutputStream();

        final DataInput dataInput = new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

        final PointWritable pointWritable = new PointWritable();
        pointWritable.readFields(dataInput);

        final Point point = pointWritable.point;

        assertEquals(123, point.getX(), 0.000001);
        assertEquals(345, point.getY(), 0.000001);
    }

    private ByteArrayOutputStream getByteArrayOutputStream() throws IOException
    {
        final PointWritable pointWritable = new PointWritable();
        
        GeometryFactory gf = new GeometryFactory();

        Coordinate coord = new Coordinate( 123, 345 );
        Point point = gf.createPoint( coord );
        
        pointWritable.point = point;   

        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final DataOutput dataOutput = new DataOutputStream(byteArrayOutputStream);
        pointWritable.write(dataOutput);
        byteArrayOutputStream.flush();
        return byteArrayOutputStream;
    }
}
