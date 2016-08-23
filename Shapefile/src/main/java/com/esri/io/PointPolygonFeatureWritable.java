
package com.esri.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 */


//new Writable to read Point and Polygon from the same ShapeFile


public class PointPolygonFeatureWritable extends PointPolygonWritable {
    
    public final Attributes attributes = new Attributes();

    public PointPolygonFeatureWritable() {        
    }
    
    @Override
    public void write(final DataOutput dataOutput) throws IOException {
        
        super.write(dataOutput);
        attributes.write(dataOutput);
        
    }

    @Override
    public void readFields(final DataInput dataInput) throws IOException {
        
        super.readFields(dataInput);
        attributes.readFields(dataInput);
        
    }
    
    
}
