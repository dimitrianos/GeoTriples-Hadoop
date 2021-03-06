
import be.ugent.mmlab.rml.core.KeyGenerator;

import com.esri.io.PointMultiPolygonFeatureWritable;
import com.esri.jts_extras.ShapeAttributeTitles;
import com.esri.mapreduce.PointMultiPolygonFeatureInputFormat;

import org.apache.commons.codec.binary.Base64;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.*;

import org.openrdf.model.Statement;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.*;

import be.ugent.mmlab.rml.core.NodeRMLPerformer;
import be.ugent.mmlab.rml.core.RMLMappingFactory;
import be.ugent.mmlab.rml.function.Config;
import be.ugent.mmlab.rml.function.FunctionArea;
import be.ugent.mmlab.rml.function.FunctionAsGML;
import be.ugent.mmlab.rml.function.FunctionAsWKT;
import be.ugent.mmlab.rml.function.FunctionCentroidX;
import be.ugent.mmlab.rml.function.FunctionCentroidY;
import be.ugent.mmlab.rml.function.FunctionCoordinateDimension;
import be.ugent.mmlab.rml.function.FunctionDimension;
import be.ugent.mmlab.rml.function.FunctionEQUI;
import be.ugent.mmlab.rml.function.FunctionFactory;
import be.ugent.mmlab.rml.function.FunctionHasSerialization;
import be.ugent.mmlab.rml.function.FunctionIs3D;
import be.ugent.mmlab.rml.function.FunctionIsEmpty;
import be.ugent.mmlab.rml.function.FunctionIsSimple;
import be.ugent.mmlab.rml.function.FunctionLength;
import be.ugent.mmlab.rml.function.FunctionSpatialDimension;
import be.ugent.mmlab.rml.model.RMLMapping;
import be.ugent.mmlab.rml.model.TriplesMap;
import be.ugent.mmlab.rml.processor.RMLProcessor;
import be.ugent.mmlab.rml.processor.RMLProcessorFactory;
import be.ugent.mmlab.rml.processor.concrete.ConcreteRMLProcessorFactory;
import net.antidot.semantic.rdf.model.impl.sesame.SesameDataSet;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.InvalidR2RMLStructureException;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.InvalidR2RMLSyntaxException;
import net.antidot.semantic.rdf.rdb2rdf.r2rml.exception.R2RMLDataError;




/**
 * Created by bard on 4/3/16.
 */



public class Hadoop_Implementation_class
{

    public static class ShapeFileMap extends
            Mapper<LongWritable, PointMultiPolygonFeatureWritable, Writable, NullWritable> {


        private final Text m_text = new Text();

        private RDFFormat format = RDFFormat.NTRIPLES;

        private SesameDataSet outputDataSet = new NullDataSet();

        private String m;

        private byte[] valueDecoded;

        private RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();

        private RMLProcessor processor;

        private NodeRMLPerformer performer;

        private Map<String,KeyGenerator> keygens = new HashMap<String,KeyGenerator>();

        private List<TriplesMap> list_map;

        private List<String> field_titles;

        private int k=0;

        private NTriplesAlternative rdfWriter = new NTriplesAlternative();


        @Override
        public void map(
                final LongWritable key,
                final PointMultiPolygonFeatureWritable val,
                final Context context) throws IOException, InterruptedException {



            if(k==0) {


                //input filename to used on geotriples id's
                String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                eu.linkedeodata.geotriples.Config.variables.put("filename",filename);


                //get the shape file's attribute titles
                ShapeAttributeTitles shp_titles = new ShapeAttributeTitles();

                field_titles = shp_titles.getAll_titles();

                Set<String> hs = new HashSet();

                hs.addAll(field_titles);
                field_titles.clear();
                field_titles.addAll(hs);


                //read the mapping file (deserialize)
                Configuration conf = context.getConfiguration();

                m = conf.get("triplemap");

                valueDecoded = Base64.decodeBase64(m.getBytes());

                    try {

                        ObjectInputStream si = new ObjectInputStream( new ByteArrayInputStream(valueDecoded) );

                        list_map = (List<TriplesMap>) si.readObject();

                        for(TriplesMap tm:list_map) {

                            keygens.put(tm.getName(),new KeyGenerator());

                        }

                    } catch (Exception e) {

                        System.out.println(e);

                    }

                registerFunctions();
                k++;

            }




            for(TriplesMap tm : list_map){


                processor = factory.create(tm.getLogicalSource().getReferenceFormulation());

                performer = new NodeRMLPerformer(processor);

                HashMap<String, Object> row = new HashMap();

                    try {

                        //write the attribute's titles
                        for(String a : field_titles) {

                            String field_value = val.attributes.getText(a).trim();

                            if(!field_value.isEmpty()) {
                                row.put(a, field_value);
                            }

                        }

                        //check the record geometry type and write the right values
                        //ShapeType_for_Hadoop==5 => MultiPolygon
                        if(val.ShapeType_for_Hadoop==5){


                            row.put("the_geom",val.multiPolygon);


                        }
                        //ShapeType_for_Hadoop==1 => Point
                        else if(val.ShapeType_for_Hadoop==1){

                            row.put("the_geom",val.point);

                        }

                        row.put(Config.GEOTRIPLES_AUTO_ID,keygens.get(tm.getName()).Generate());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                Collection<Statement> statements = performer.perform(row, outputDataSet,tm);


                    try {

                        rdfWriter.startRDF();


                            for(Statement st : statements){

                                rdfWriter.handleStatement(st);


                            }
                           

                        

                        m_text.set(rdfWriter.getString());
                        rdfWriter.endRDF();
                       // m_text.set(statements_str.substring(0,statements_str.length()-1));

                        context.write(m_text, NullWritable.get());

                    } catch (RDFHandlerException e) {


                        e.printStackTrace();
                    }

            }


        }

    }


    //csv
    public static class CsvFileMap extends
            Mapper<LongWritable, Text, Writable, NullWritable> {


        private final Text m_text = new Text();

        private List<String> field_titles;

        private List<String> field_values;

        private String m;

        private String header_values;

        private byte[] valueDecoded;

        private Map<String,KeyGenerator> keygens = new HashMap<String,KeyGenerator>();

        private RMLProcessorFactory factory = new ConcreteRMLProcessorFactory();

        private RMLProcessor processor;

        private NodeRMLPerformer performer;

        private List<TriplesMap> list_map;

        private RDFFormat format = RDFFormat.NTRIPLES;

        private SesameDataSet outputDataSet = new NullDataSet();

        private int k=0;

        private NTriplesAlternative rdfWriter = new NTriplesAlternative();



        public void map(
                final LongWritable key,
                final Text val,
                final Context context) throws IOException, InterruptedException {


            String line = val.toString();

            long key_value = key.get();


            if(key_value!=0){


                //read titles and triples map
                if (k == 0) {

                    //input filename to used on geotriples id's
                    String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

                    eu.linkedeodata.geotriples.Config.variables.put("filename", filename + "_" + key_value);


                    //read the mapping file (deserialize) and headers
                    Configuration conf = context.getConfiguration();

                    header_values = conf.get("header_values");

                    field_titles = Arrays.asList(header_values.split(","));

                    m = conf.get("triplemap");

                    valueDecoded = Base64.decodeBase64(m.getBytes());

                    try {

                        ObjectInputStream si = new ObjectInputStream(new ByteArrayInputStream(valueDecoded));

                        list_map = (List<TriplesMap>) si.readObject();

                        for (TriplesMap tm : list_map) {

                            keygens.put(tm.getName(), new KeyGenerator());

                        }

                    } catch (Exception e) {

                        System.out.println(e);

                    }

                    registerFunctions();
                    k++;


                }



                for (TriplesMap tm : list_map) {

                    processor = factory.create(tm.getLogicalSource().getReferenceFormulation());

                    performer = new NodeRMLPerformer(processor);

                    HashMap<String, Object> row = new HashMap();

                    try {

                        //write the attribute's titles and values
                        field_values = Arrays.asList(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));


                        int field_titles_index = 0;


                        for (String a : field_titles) {

                            row.put(a, field_values.get(field_titles_index).replaceAll("\"", ""));

                            field_titles_index++;

                        }

                        row.put(Config.GEOTRIPLES_AUTO_ID, keygens.get(tm.getName()).Generate());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    Collection<Statement> statements = performer.perform(row, outputDataSet, tm);



                    try {

                        rdfWriter.startRDF();


                        for (Statement st : statements) {

                            rdfWriter.handleStatement(st);

                        }

                        m_text.set(rdfWriter.getString());
                        rdfWriter.endRDF();

                        context.write(m_text, NullWritable.get());

                    } catch (RDFHandlerException e) {


                        e.printStackTrace();
                    }



                }

            }

        }
    }



    public static class GeneralReduce
            extends Reducer<Text, NullWritable,Writable,NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {



            context.write(key, NullWritable.get());
        }

    }





    public static void registerFunctions() {
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/equi"),
                new FunctionEQUI()); // don't remove or change this line, it
        // replaces the equi join functionality
        // of R2RML

        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/asWKT"),
                new FunctionAsWKT());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/hasSerialization"),
                new FunctionHasSerialization());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/asGML"),
                new FunctionAsGML());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/isSimple"),
                new FunctionIsSimple());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/isEmpty"),
                new FunctionIsEmpty());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/is3D"),
                new FunctionIs3D());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/spatialDimension"),
                new FunctionSpatialDimension());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/dimension"),
                new FunctionDimension());
        FunctionFactory.registerFunction(
                new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/coordinateDimension"),
                new FunctionCoordinateDimension());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/area"),
                new FunctionArea());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/length"),
                new FunctionLength());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/centroidx"),
                new FunctionCentroidX());
        FunctionFactory.registerFunction(new URIImpl("http://www.w3.org/ns/r2rml-ext/functions/def/centroidy"),
                new FunctionCentroidY());

    }

        public static List<String> all_triples_maps(String mapping_file_path)
                throws RepositoryException, R2RMLDataError, RDFParseException, IOException, InvalidR2RMLSyntaxException, InvalidR2RMLStructureException {

            List<String> source_and_mapping = new ArrayList<>();

            String input_dir="default";

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(mapping_file_path);
            FSDataInputStream in = fs.open(inFile);
            RMLMapping mapping = RMLMappingFactory.extractRMLMapping(in);


            Config.EPSG_CODE = "4326";

            String encoded = null;
            ArrayList<TriplesMap> list=new ArrayList<TriplesMap>();

            for (TriplesMap m : mapping.getTriplesMaps()) {

                input_dir = m.getLogicalSource().getIdentifier();

                list.add(m);

            }


                    // serialize the object
                    try {
                        ByteArrayOutputStream bo = new ByteArrayOutputStream();
                        ObjectOutputStream so = new ObjectOutputStream(bo);
                        so.writeObject(list);
                        so.flush();

                        encoded = new String(Base64.encodeBase64(bo.toByteArray()));
                    } catch (Exception e) {
                        System.out.println(e);
                    }


            //encoded mapping file
            source_and_mapping.add(encoded);
            //logical source
            source_and_mapping.add(input_dir);



            return source_and_mapping;
        }



    public static void main(String[] args) throws Exception {



        if(args.length>0) {

            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(conf);
            Path inFile = new Path(args[0]);
            FSDataInputStream in = fs.open(inFile);


            conf.addResource(in);


            //shapefile
            if(conf.get("files_format").equals("shp")) {

                //read mapping file (serialize) and pass it to configuration
                List<String> source_and_mapping = all_triples_maps(conf.get("mapping_file"));


                conf.set("triplemap", source_and_mapping.get(0));

                Job job = Job.getInstance(conf, "geotriples");

                job.setJarByClass(Hadoop_Implementation_class.class);
                job.setMapperClass(ShapeFileMap.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);


                job.setNumReduceTasks(Integer.parseInt(conf.get("reducers_number")));

                job.setReducerClass(GeneralReduce.class);

                job.setInputFormatClass(PointMultiPolygonFeatureInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                //override or retain the mapping file input directory
                if(conf.get("input_dir").equals("default")) {

                    FileInputFormat.addInputPath(job, new Path(source_and_mapping.get(1)));

                }
                else {

                    FileInputFormat.addInputPath(job, new Path(conf.get("input_dir")));

                }
//            //FileInputFormat.setInputDirRecursive(job,true);
                FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));



                job.waitForCompletion(true);


                TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);

                System.out.println(map_reports.length);
                for(TaskReport report : map_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
                }

                TaskReport[] reduce_reports = job.getTaskReports(TaskType.REDUCE);

                System.out.println(reduce_reports.length);
                for(TaskReport report : reduce_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
                }




                if(conf.get("merge").equals("1")) {


                FileSystem fileSystem = FileSystem.get(conf);


                Path source = new Path(conf.get("output_dir"));

                Path new_target = new Path(conf.get("output_dir")+"_new");

                fileSystem.mkdirs(new_target);

                Path target = new Path(conf.get("output_dir")+"_new");

                FileUtil.copyMerge(fileSystem, source,fileSystem, target, true , conf, null);

                fileSystem.rename(target,source);



            }

            }
            //csv
            else if(conf.get("files_format").equals("csv")) {

                //read mapping file (serialize) and pass it to configuration
                List<String> source_and_mapping = all_triples_maps(conf.get("mapping_file"));


                //override or retain the mapping file input directory
                if(conf.get("input_dir").equals("default")) {

                    FileStatus[] status = fs.listStatus(new Path(source_and_mapping.get(1)));

                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));

                    String header_values = br.readLine();

                    conf.set("header_values",header_values);

                }
                else {

                    FileStatus[] status = fs.listStatus(new Path(conf.get("input_dir")));

                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[0].getPath())));

                    String header_values = br.readLine();

                    conf.set("header_values",header_values);
                }



                conf.set("triplemap", source_and_mapping.get(0));


                Job job = Job.getInstance(conf, "geotriples");

                job.setJarByClass(Hadoop_Implementation_class.class);
                job.setMapperClass(CsvFileMap.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(NullWritable.class);

                job.setNumReduceTasks(Integer.parseInt(conf.get("reducers_number")));

                job.setReducerClass(GeneralReduce.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);


                //override or retain the mapping file input directory
                if(conf.get("input_dir").equals("default")) {

                    FileInputFormat.addInputPath(job, new Path(source_and_mapping.get(1)));

                }
                else {

                    FileInputFormat.addInputPath(job, new Path(conf.get("input_dir")));

                }
//            //FileInputFormat.setInputDirRecursive(job,true);
                FileOutputFormat.setOutputPath(job, new Path(conf.get("output_dir")));



                job.waitForCompletion(true);


                TaskReport[] map_reports = job.getTaskReports(TaskType.MAP);

                System.out.println(map_reports.length);
                for(TaskReport report : map_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Map: " + report.getTaskId() + " took " + time + " millis!");
                }

                TaskReport[] reduce_reports = job.getTaskReports(TaskType.REDUCE);

                System.out.println(reduce_reports.length);
                for(TaskReport report : reduce_reports) {

                    long time = report.getFinishTime() - report.getStartTime();
                    System.out.println("Reduce: " + report.getTaskId() + " took " + time + " millis!");
                }


                if(conf.get("merge").equals("1")) {


                    FileSystem fileSystem = FileSystem.get(conf);


                    Path source = new Path(conf.get("output_dir"));

                    Path new_target = new Path(conf.get("output_dir")+"_new");

                    fileSystem.mkdirs(new_target);

                    Path target = new Path(conf.get("output_dir")+"_new");

                    FileUtil.copyMerge(fileSystem, source,fileSystem, target, true , conf, null);

                    fileSystem.rename(target,source);



                }

            }
            else {

                System.out.println("Please provide correct file format");
                System.exit(0);

            }


        }
        else {

            System.out.println("Please provide the xml configuration file");
            System.exit(0);

        }



    }

}


