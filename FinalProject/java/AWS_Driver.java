//
//    CS755 Final Project
//
//    Anuj Sampat
//    asampat@bu.edu
//
//    AWS_Driver.java: AWS driver to connect to an Amazon EMR cluster, submit a MR job and download
//    the MR job output from Amazon S3 storage to local disk for presentation to the user.
//
//

package bu.as.cs755;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Random;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeJobFlowsRequest;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowDetail;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;


public class AWS_Driver
{
    private static PrintWriter out;
    private static PrintWriter outArgs;
    private static String clusterID    = "j-3EO2AYAEU9KO3";
    private static String songListPath = "/var/www/html/";
    private static String s3BucketName = "anuj-cs755";
    private static String s3BucketPath = "s3://anuj-cs755/";
    private static String s3BucketIn   = s3BucketPath + "input";

    //private static String s3BucketIn = "s3://tbmmsd";
    private static String s3BucketOut = s3BucketPath + "output";
    private static String msdJAR = "s3://anuj-cs755/msd/MSD.jar";

    public static void main(String[] args)
    {
        songListPath += "songList";
        songListPath += args[args.length - 1];

        try {
            outArgs = new PrintWriter("/var/www/html/args.txt");

            for (int i = 0; i < args.length; i++)
            {
                outArgs.println(args[i]);
            }

            outArgs.close();
        }
        catch (Exception ex)
        {
            System.out.println("failed");
        }

        Random randomGenerator = new Random();
        int    outputNum = randomGenerator.nextInt(10000);

        AWSCredentials credentials = null;
        try {
            InputStream isCred = ClassLoader.getSystemClassLoader().getResourceAsStream("bu/as/cs755/rootkey.csv");
            credentials = new PropertiesCredentials(isCred);
        } catch (Exception e1) {
            System.out.println("could not create credentials");
        }

        AmazonElasticMapReduce client = new AmazonElasticMapReduceClient(credentials);

        String outputPath = s3BucketOut + outputNum;
        String[] msdArgs = {s3BucketIn, outputPath, args[1], args[2], args[3], args[4], args[5]};

        // A custom step
        HadoopJarStepConfig hadoopConfig1 = new HadoopJarStepConfig()
                                            .withJar(msdJAR)
                                            .withMainClass("bu.as.cs755.MSD_Driver") // optional main class, this can be omitted if jar above has a manifest
                                            .withArgs(msdArgs); // optional list of arguments
        StepConfig customStep = new StepConfig("Step1", hadoopConfig1);

        StepConfig[] theSteps = {customStep};

        AddJobFlowStepsResult result = client.addJobFlowSteps(new AddJobFlowStepsRequest()
                                                              .withJobFlowId(clusterID).withSteps(theSteps));
        System.out.println(result.getStepIds());

        while (true)
        {
            String state = "no state";
            DescribeJobFlowsRequest jobAttributes = new DescribeJobFlowsRequest();
            List<JobFlowDetail>     jobs = client.describeJobFlows(jobAttributes).getJobFlows();

            if (jobs.size() != 0)
            {
                JobFlowDetail detail = jobs.get(0);
                state = detail.getExecutionStatusDetail().getState();
                System.out.println(state);

                if (state.equalsIgnoreCase("running"))
                {
                    break;
                }
            }
            else
            {
                System.out.println("waiting for job to start current state " + state);
            }

            try {
                Thread.sleep(5000);
            }
            catch (Exception ex) {}
        }

        while (true)
        {
            String state = "no state";
            DescribeJobFlowsRequest jobAttributes = new DescribeJobFlowsRequest();

            List<JobFlowDetail> jobs = client.describeJobFlows(jobAttributes).getJobFlows();

            if (jobs.size() != 0)
            {
                JobFlowDetail detail = jobs.get(0);
                state = detail.getExecutionStatusDetail().getState();
                System.out.println(state);

                if (state.equalsIgnoreCase("waiting"))
                {
                    break;
                }
            }
            else
            {
                System.out.println("waiting for job to finish current state " + state);
            }

            try {
                Thread.sleep(5000);
            }
            catch (Exception ex) {}
        }

        AmazonS3Client s3Client = new AmazonS3Client(credentials);

        String prefixStr = "output" + outputNum + "/part";
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                                                .withBucketName(s3BucketName).withPrefix(prefixStr);


        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

        S3ObjectSummary objectSummary;
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();

        try {
            out = new PrintWriter(songListPath);
        }
        catch (Exception ex)
        {
            System.out.println("failed to open file");
            return;
        }

        for (int i = 0; i < objectSummaries.size(); i++)
        {
            objectSummary = objectSummaries.get(i);
            System.out.println(" - " + objectSummary.getKey() + "  " +
                               "(size = " + objectSummary.getSize() +
                               ")");

            try {
                System.out.println("Downloading an object");
                S3Object s3object = s3Client.getObject(new GetObjectRequest(
                                                           s3BucketName, objectSummary.getKey()));
                System.out.println("Content-Type: " +
                                   s3object.getObjectMetadata().getContentType());
                displayTextInputStream(s3object.getObjectContent());
            }
            catch (IOException ioEx)
            {
                System.out.println("failed to open file");
                return;
            }
        }

        out.close();
    }

    private static void displayTextInputStream(InputStream input) throws IOException
    {
        // Read one text line at a time and display.
        if (input == null)
        {
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        while (true)
        {
            String line = reader.readLine();

            if (line == null)
            {
                break;
            }

            out.println(line);
        }
    }
}
