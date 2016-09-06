
package com.esri.mapreduce;

import com.esri.io.PointMultiPolygonFeatureWritable;
import com.esri.jts_extras.PointorMultiPolygon;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 */

//new input format for reading points and polygons from the same file
public class PointMultiPolygonFeatureInputFormat extends AbstractInputFormat<PointMultiPolygonFeatureWritable>
{
    
    private final class PointMultiPolygonFeatureReader extends AbstractFeatureReader<PointMultiPolygonFeatureWritable>
    {
    
        private final PointMultiPolygonFeatureWritable m_pointMultipolygonFeatureWritable = new PointMultiPolygonFeatureWritable();
    
        public PointMultiPolygonFeatureReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {         
            
            initialize(inputSplit, taskAttemptContext);
            
        }
     
     
        @Override
        protected void next() throws IOException
        {            
        
            m_shpReader.PointorMultiPolygon_Selector(m_pointMultipolygonFeatureWritable.point);



            if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==5)
            {

                m_pointMultipolygonFeatureWritable.multiPolygon = PointorMultiPolygon.getMultiPolygonPnorMlPl();

                m_pointMultipolygonFeatureWritable.ShapeType_for_Hadoop=5;

                putAttributes(m_pointMultipolygonFeatureWritable.attributes);

            }
            else if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==1)
            {

                m_pointMultipolygonFeatureWritable.point = PointorMultiPolygon.getPointPnorMlPl();

                m_pointMultipolygonFeatureWritable.ShapeType_for_Hadoop=1;

                putAttributes(m_pointMultipolygonFeatureWritable.attributes);

            }

            
        }
        
        
        @Override
        public PointMultiPolygonFeatureWritable getCurrentValue() throws IOException, InterruptedException
        {                     
           
            return m_pointMultipolygonFeatureWritable;
            
        }
        
    }
    
    
    @Override
    public RecordReader<LongWritable, PointMultiPolygonFeatureWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        
        return new PointMultiPolygonFeatureReader(inputSplit, taskAttemptContext);
        
    }
    
}
