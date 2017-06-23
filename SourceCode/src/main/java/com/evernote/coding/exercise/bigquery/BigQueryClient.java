package com.evernote.coding.exercise.bigquery;

import com.google.cloud.bigquery.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by SuryaSelvaraj on 6/17/17.
 */
public class BigQueryClient {

    static double globalMean = 0.0;
    static List<Double> medianList = new ArrayList<>();

    public static void main(String... args) throws Exception {
        final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        //Execute query
        QueryExecutor queryExecutorTask = new QueryExecutor(bigquery);

        /**
         * Schedule query execution every hour. To schedule for every
         * 24 hours, 24*60*60*1000L
         */

        Timer timer = new Timer();
        timer.scheduleAtFixedRate(queryExecutorTask, 0L, 60*60*1000L);

    }

}
