
package com.esri.io;


import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateFilter;
import com.vividsolutions.jts.geom.GeometryFactory;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class PointWritable implements Writable
{
    public Point point;

    public PointWritable()
    {
        
        //Create a point
        GeometryFactory gf = new GeometryFactory();

        Coordinate coord = new Coordinate(0,0);
        point= gf.createPoint(coord);    
       
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {
                
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
               
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        //apply the new Coordinate to point variable
        point.apply(new CoordinateFilter() {
        
            @Override
            public void filter(Coordinate coord) {
                try {

                    coord.x = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointWritable.class.getName()).log(Level.SEVERE, null, ex);

                }

                try {

                    coord.y = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointWritable.class.getName()).log(Level.SEVERE, null, ex);

                }

            }

        });
               

    }
    
    
}
