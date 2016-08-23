package com.esri.io;

    
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
    
    
import com.esri.jts_extras.JtsPolygon;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 */
public class PolygonWritable implements Writable
{
    
    public Polygon polygon;
        

    public PolygonWritable() {          
    
    
        JtsPolygon jtspl = new JtsPolygon();
               
        //check if polygon is not null, if null create a new one
        //
        if(jtspl.my_polygon()!=null){
               
            polygon = jtspl.my_polygon();
         
        }
        else
        {
     
            GeometryFactory gf=new GeometryFactory();
        
            Coordinate[] coords  =
            new Coordinate[] {new Coordinate(1, 0), new Coordinate(10, 0),
                            new Coordinate(10, 10),  new Coordinate(1,0)};
            LinearRing ring = gf.createLinearRing( coords );
            LinearRing holes[] = null;        
    
            polygon = gf.createPolygon(ring, holes);
            jtspl.set_my_polygon(polygon);
     
        }
        
        
    }

    @Override
    public void write(final DataOutput dataOutput) throws IOException
    {            
        //jts library polygon 
        LineString ls = polygon.getExteriorRing();
               
        
        int ls_points = ls.getNumPoints();       
       
        dataOutput.writeInt(ls_points);
        
        Point point;
        
        for(int i = 0; i<ls_points; i++)
        {
        
            point = ls.getPointN(i); 
            
            dataOutput.writeDouble(point.getX());
            dataOutput.writeDouble(point.getY());
            
        }        
        
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException
    {       
        
       //jts library polygon 
        int num_points = dataInput.readInt();
        
        Coordinate[] coords  = new Coordinate[num_points];
        
        for(int i=0; i < num_points; i++){
        
            coords[i] =  new Coordinate(dataInput.readDouble(),dataInput.readDouble());
        
        }
        
        GeometryFactory gf=new GeometryFactory();
        
        LinearRing ring = gf.createLinearRing(coords);
        
        LinearRing holes[] = null;
        
        polygon = gf.createPolygon(ring, holes);
                
        JtsPolygon mpl = new JtsPolygon();
        
        mpl.set_my_polygon(polygon);       
            
        
    }
    
    
}
