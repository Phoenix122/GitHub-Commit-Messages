package com.evernote.coding.exercise.bigquery;

import com.google.cloud.bigquery.*;

import java.util.*;
import java.util.concurrent.*;

import static com.evernote.coding.exercise.bigquery.BigQueryClient.*;

/**
 * Created by SuryaSelvaraj on 6/19/17.
 */
public class QueryExecutor extends TimerTask {

    private final BigQuery bigquery;
    public QueryExecutor(BigQuery bigQuery) {
        this.bigquery = bigQuery;
    }
    @Override
    public void run() {
        System.out.println("Run Me ");

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Set<Callable<Double>> callables = new HashSet<>();

        callables.add(new Callable<Double>() {
            public Double call() throws Exception {
                final JobId meanJobId = executeQuery(QueryBuilder.getQueryConfig("MEAN"));
                QueryResponse response = bigquery.getQueryResults(meanJobId);
                QueryResult meanResult = response.getResult();
                processMean(meanResult);
                System.out.println("********* Adjusted Mean: ***************** " + globalMean);
                return globalMean;
            }
        });
        callables.add(new Callable<Double>() {
            public Double call() throws Exception {
                final JobId medianJobId = executeQuery(QueryBuilder.getQueryConfig("MEDIAN"));
                QueryResponse response = bigquery.getQueryResults(medianJobId);
                QueryResult medianResult = response.getResult();
                double currentMedian = processMedian(medianResult);
                System.out.println("*********** Adjusted Median: ************** " + currentMedian);
                return currentMedian;
            }
        });

        List<Future<Double>> futures = null;
        try {
            futures = executorService.invokeAll(callables);
            /**for(Future<Double> future : futures){
             System.out.println("future.get = " + future.get());
             }*/
        } catch (InterruptedException e) {
            e.printStackTrace();
        } /** catch (ExecutionException e) {
         e.printStackTrace();
         }*/

        executorService.shutdown();

    }

    /**
     * This method adjusts the old mean with the updated data.
     * @param result
     */
    private void processMean(QueryResult result) {
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                double mean = row.get(0).getDoubleValue();
                //System.out.println(mean);
                if(BigQueryClient.globalMean != 0.0) {
                    globalMean = (globalMean +mean)/2;
                } else {
                    globalMean = mean;
                }
            }
            result = result.getNextPage();
        }
    }

    /**
     *
     * This method adjusts the median with the updated data.
     * @param result
     * @return
     */
    private double processMedian(QueryResult result) {
        double globalMedian = 0.0;
        while (result != null) {
            for (List<FieldValue> row : result.iterateAll()) {
                double median = row.get(0).getDoubleValue();
                //System.out.println(median);
                medianList.add(median);
                Collections.sort(medianList);
                int midElementIdx = medianList.size()/2;
                globalMedian = (medianList.size()%2 == 0)?(medianList.get(midElementIdx)+medianList.get(midElementIdx-1))/2:medianList.get(midElementIdx);
            }
            result = result.getNextPage();
        }
        return globalMedian;
    }

    /**
     *
     * Executes the query submitted to the job.
     * @param queryConfig
     * @return
     * @throws InterruptedException
     * @throws TimeoutException
     */
    private JobId executeQuery(QueryJobConfiguration queryConfig) throws InterruptedException, TimeoutException {
        //create a job for querying
        final JobId medianJobId = JobId.of(UUID.randomUUID().toString());
        Job medianQueryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(medianJobId).build());
        // Wait for the query to complete.
        medianQueryJob = medianQueryJob.waitFor();
        handleJobErrors(medianQueryJob);
        return medianJobId;
    }

    /**
     * Handles any errors in the job executing the query.
     * @param job
     */
    private void handleJobErrors(Job job) {
        // Check for errors
        if (job == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (job.getStatus().getError() != null) {
            throw new RuntimeException(job.getStatus().getError().toString());
        }
    }
}
