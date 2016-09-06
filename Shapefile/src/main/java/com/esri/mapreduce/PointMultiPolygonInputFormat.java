
package com.esri.mapreduce;

import com.esri.io.PointMultiPolygonWritable;
import com.esri.jts_extras.PointorMultiPolygon;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


/**
 */

//new input format for reading points and polygons from the same file
public class PointMultiPolygonInputFormat extends AbstractInputFormat<PointMultiPolygonWritable>
{
    
    private final class PointMultiPolygonReader extends AbstractFeatureReader<PointMultiPolygonWritable>
    {
    
        private final PointMultiPolygonWritable m_pointMultipolygonWritable = new PointMultiPolygonWritable();
    
        public PointMultiPolygonReader(
                final InputSplit inputSplit,
                final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {         
            
            initialize(inputSplit, taskAttemptContext);
            
        }
     
     
        @Override
        protected void next() throws IOException
        {            
        
            m_shpReader.PointorMultiPolygon_Selector(m_pointMultipolygonWritable.point);



            if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==5)
            {

                m_pointMultipolygonWritable.multiPolygon = PointorMultiPolygon.getMultiPolygonPnorMlPl();

                m_pointMultipolygonWritable.ShapeType_for_Hadoop=5;

            }
            else if(PointorMultiPolygon.getMultiPolygonorPoint_Shapetype()==1)
            {

                m_pointMultipolygonWritable.point = PointorMultiPolygon.getPointPnorMlPl();

                m_pointMultipolygonWritable.ShapeType_for_Hadoop=1;

            }
            
        }
        
        
        @Override
        public PointMultiPolygonWritable getCurrentValue() throws IOException, InterruptedException
        {
           
            return m_pointMultipolygonWritable;
            
        }
        
    }
    
    
    @Override
    public RecordReader<LongWritable, PointMultiPolygonWritable> createRecordReader(
            final InputSplit inputSplit,
            final TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        
        return new PointMultiPolygonReader(inputSplit, taskAttemptContext);
        
    }
    
}
