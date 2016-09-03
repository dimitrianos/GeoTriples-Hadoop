
package com.esri.mapreduce;

import com.esri.io.PointPolygonFeatureWritable;
import com.esri.jts_extras.PointorPolygon;
import com.esri.jts_extras.PointorMultiPolygon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 */

//new input format for reading points and polygons from the same file
public class PointPolygonFeatureInputFormat extends AbstractInputFormat<PointPolygonFeatureWritable>
{
    
    private final class PointPolygonFeatureReader extends AbstractFeatureReader<PointPolygonFeatureWritable>
    {
    
        private final PointPolygonFeatureWritable m_pointpolygonFeatureWritable = new PointPolygonFeatureWritable();
    
        public PointPolygonFeatureReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {         
            
            initialize(inputSplit, taskAttemptContext);
            
        }
     
     
        @Override
        protected void next() throws IOException
        {            
        
            m_shpReader.PointorPolygon_Selector(m_pointpolygonFeatureWritable.point);
               
            //check the type of geometry on the record, query the geometry and set the right values
//            if(PointorPolygon.getPolygonorPoint_Shapetype()==5)
//            {
//
//                m_pointpolygonFeatureWritable.polygon = PointorPolygon.getPolygonPnorPl();
//
//                m_pointpolygonFeatureWritable.ShapeType_for_Hadoop=5;
//
//                putAttributes(m_pointpolygonFeatureWritable.attributes);
//
//            }
//            else if(PointorPolygon.getPolygonorPoint_Shapetype()==1)
//            {
//
//               m_pointpolygonFeatureWritable.point = PointorPolygon.getPointPnorPl();
//
//               m_pointpolygonFeatureWritable.ShapeType_for_Hadoop=1;
//
//               putAttributes(m_pointpolygonFeatureWritable.attributes);
//
//            }




            if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==5)
            {

                m_pointpolygonFeatureWritable.multiPolygon = PointorMultiPolygon.getMultiPolygonPnorMlPl();

                m_pointpolygonFeatureWritable.ShapeType_for_Hadoop=5;

                putAttributes(m_pointpolygonFeatureWritable.attributes);

            }
            else if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==1)
            {

                m_pointpolygonFeatureWritable.point = PointorMultiPolygon.getPointPnorMlPl();

                m_pointpolygonFeatureWritable.ShapeType_for_Hadoop=1;

                putAttributes(m_pointpolygonFeatureWritable.attributes);

            }



      
            
        }
        
        
        @Override
        public PointPolygonFeatureWritable getCurrentValue() throws IOException, InterruptedException
        {                     
           
            return m_pointpolygonFeatureWritable;
            
        }
        
    }
    
    
    @Override
    public RecordReader<LongWritable, PointPolygonFeatureWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        
        return new PointPolygonFeatureReader(inputSplit, taskAttemptContext);
        
    }
    
}
