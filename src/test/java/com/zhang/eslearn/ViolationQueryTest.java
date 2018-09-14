/**
 * 
 */
package com.zhang.eslearn;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalHistogram;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author zjie
 *
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class ViolationQueryTest {
    
    private static final String INDEX = "a3-reporting-violation-client-2018.09";
    private static final String INDEX_2 = "a3-reporting-violation-client-*";
    private static final String DOC = "a3-reporting-violation-client";

    @Autowired
    private TransportClient client;
    
    @Test
    public void aggsTermsQuery_1() {
        long dateMil = TimeUnit.DAYS.toMillis(1);
        long toDate = System.currentTimeMillis();
        long fromDate = toDate - TimeUnit.DAYS.toMillis(1);
        int timeZone = 8;
        
        long fromDateUtc = fromDate - fromDate%dateMil - TimeUnit.HOURS.toMillis(timeZone);
        long toDateUtc = toDate - toDate%dateMil - TimeUnit.HOURS.toMillis(timeZone) + dateMil -1;
        
        System.out.println(String.format("From %d to %d", fromDateUtc, toDateUtc));
        
        SearchResponse response = client.prepareSearch(INDEX_2)
                .setTypes(DOC)
                .setSize(0)
                .setQuery( QueryBuilders.termQuery("ownerId", 102L) )
                .setQuery( QueryBuilders.termQuery("orgId", 0L) )
                .setQuery( QueryBuilders.rangeQuery("startDate").gte(fromDateUtc) )
                .setQuery( QueryBuilders.rangeQuery("startDate").lt(toDateUtc) )
                .addAggregation(
                        AggregationBuilders
                                .dateHistogram("date")
                                .field("startDate")
                                .dateHistogramInterval(DateHistogramInterval.DAY)
                                .offset(TimeUnit.HOURS.toMillis(timeZone) * -1)
//                                .offset(String.valueOf(timeZone * -1) + "h")
                                .minDocCount(0)
                                .extendedBounds( new ExtendedBounds(fromDateUtc+1, toDateUtc-1) )
//                                .subAggregation(
//                                        AggregationBuilders
//                                            .count("counts")
//                                            .field("ownerId")
//                                )
                 )
                .get();
        
        Map<String, Aggregation> aggregationMap = response.getAggregations().asMap();
        InternalDateHistogram dateGroup = (InternalDateHistogram)aggregationMap.get("date");
        dateGroup.getBuckets().forEach( item -> {
            System.out.println(item.getKeyAsString() + " : " + item.getDocCount());
        });
        
        System.out.println(response.getAggregations().asMap());
        System.out.println(response);
    }
    
    @Test
    public void aggsTermsQuery_2() {
        SearchResponse response = client.prepareSearch(INDEX_2)
                .setTypes(DOC)
                .setSize(0)
                .setQuery( QueryBuilders.termQuery("ownerId", 102L) )
                .setQuery( QueryBuilders.termQuery("orgId", 0L) )
                .addAggregation(
                        AggregationBuilders
                                .histogram("date")
                                .field("startDate")
                                .interval(TimeUnit.DAYS.toMillis(1))
//                                .offset(-20)
                                .minDocCount(0)
                 )
                .get();
        
        Map<String, Aggregation> aggregationMap = response.getAggregations().asMap();
        InternalHistogram dateGroup = (InternalHistogram)aggregationMap.get("date");
        dateGroup.getBuckets().forEach( item -> {
            System.out.println(item.getKey() + " : " + item.getDocCount());
        });
        
        System.out.println(response.getAggregations().asMap());
        System.out.println(response);
    }
}
