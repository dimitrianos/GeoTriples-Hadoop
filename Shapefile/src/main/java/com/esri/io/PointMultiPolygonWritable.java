
package com.esri.io;


import com.vividsolutions.jts.geom.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.Writable;

/**
 */

public class PointMultiPolygonWritable implements Writable{


    public MultiPolygon multiPolygon;
    
    public Point point;
    
    public int ShapeType_for_Hadoop;
    
    
    public PointMultiPolygonWritable()
    {  
        
        //Create a point
        GeometryFactory gf_point = new GeometryFactory();

        Coordinate coord = new Coordinate(0,0);
        point= gf_point.createPoint(coord);
        


    
    }
    
    
    
    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {            

        //point case
        dataOutput.writeDouble(point.getX());
        dataOutput.writeDouble(point.getY());
        


    }
    
    
    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {
        
        //point case
        point.apply(new CoordinateFilter() {
        
            @Override
            public void filter(Coordinate coord) {

                try {

                    coord.x = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointMultiPolygonWritable.class.getName()).log(Level.SEVERE, null, ex);

                }

                try {

                    coord.y = dataInput.readDouble();

                } catch (IOException ex) {

                    Logger.getLogger(PointMultiPolygonWritable.class.getName()).log(Level.SEVERE, null, ex);

                }


            }

        });

        
    }
    
    
}
