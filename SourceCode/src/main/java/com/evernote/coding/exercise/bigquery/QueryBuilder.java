package com.evernote.coding.exercise.bigquery;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.UserDefinedFunction;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by SuryaSelvaraj on 6/19/17.
 */
public class QueryBuilder {

    private static final String MEAN_QUERY = "SELECT AVG(mean) FROM findMean( select JSON_EXTRACT(payload, '$.commits') AS pLoad " +
            "FROM [githubarchive:day.$tableName] " +
            "WHERE type = 'PushEvent' and payload is not null " +
            "AND TIMESTAMP(created_at) > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -60, 'MINUTE'));";

    private static final String MEDIAN_QUERY = "SELECT NTH(501, QUANTILES(median, 1001)) FROM findMedian( select JSON_EXTRACT(payload, '$.commits') AS pLoad " +
            "FROM [githubarchive:day.$tableName] " +
            "WHERE type = 'PushEvent' and payload is not null " +
            "AND TIMESTAMP(created_at) > DATE_ADD(USEC_TO_TIMESTAMP(NOW()), -60, 'MINUTE'));";

    /**
     *
     * This method builds the query configs.
     * @param queryType - mean or median
     * @return
     */
    public static QueryJobConfiguration getQueryConfig(String queryType) {
        String query = null;
        switch (queryType) {
            case "MEAN":
                query = MEAN_QUERY;
                break;
            case "MEDIAN":
                query = MEDIAN_QUERY;
                break;
        }
        query  = query.replace("$tableName", getTableName());
        System.out.println("Query is: " + query);
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query)
                        .setUseLegacySql(true)
                        .setUserDefinedFunctions(getUDFList())
                        .build();
        return queryConfig;
    }

    /**
     * Computing Table name
     * @return tableName string
     */
    private static String getTableName() {

        //Current UTC TIMESTAMP in YYYYMMDD format
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss", Locale.US);
        dateFormat.setTimeZone(timeZone);
        //System.out.println(dateFormat.format(calendar.getTime()));
        String current_date = dateFormat.format(calendar.getTime());
        System.out.println("Current time: " + current_date);
        System.out.println("Current hour: " + calendar.get(Calendar.HOUR_OF_DAY));
        //current time is 00:00:00
        if(calendar.get(Calendar.HOUR_OF_DAY) == 00) {
            calendar.add(Calendar.DATE, -1);
            current_date = dateFormat.format(calendar.getTime());
            System.out.println(current_date);
        }
        //Extracting table name
        String[] tableName = current_date.split(" ");
        System.out.println("Table queried: " + tableName[0]);
        return tableName[0];
    }

    private static List<UserDefinedFunction> getUDFList() {
        final List<UserDefinedFunction> udfList = new ArrayList<>();
        UserDefinedFunction udf_mean = UserDefinedFunction.fromUri("gs://mybucket2209/udf_mean.js");
        UserDefinedFunction udf_median = UserDefinedFunction.fromUri("gs://mybucket2209/udf_median.js");
        udfList.add(udf_mean);
        udfList.add(udf_median);
        return udfList;
    }


}
