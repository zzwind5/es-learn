/**
 * 
 */
package com.zhang.eslearn;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
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
        
//        long fromDate = 1536814800000L;
//        long toDate   = 1536987600000L;
        
        long fromDate = System.currentTimeMillis() - dateMil;
        long toDate   = System.currentTimeMillis();
        
        int timeZone = 8;
        
        long offset = TimeUnit.HOURS.toMillis(timeZone);
        
        long fromStart = getDateStartTime(fromDate, timeZone);
        long toEnd = getDateEndTime(toDate, timeZone);
        
        System.out.println(String.format("From %d to %d", fromStart, toEnd));
        
        SearchResponse response = client.prepareSearch(INDEX_2)
                .setTypes(DOC)
                .setSize(0)
                .setQuery( QueryBuilders.termQuery("ownerId", 102L) )
                .setQuery( QueryBuilders.termQuery("orgId", 0L) )
                .setQuery( QueryBuilders.rangeQuery("startDate").gte(fromStart) )
                .setQuery( QueryBuilders.rangeQuery("startDate").lte(toEnd) )
                .addAggregation(
                        AggregationBuilders
                                .dateHistogram("date")
                                .field("startDate")
                                .dateHistogramInterval(DateHistogramInterval.DAY)
                                .offset(offset * -1)
//                                .offset(String.valueOf(timeZone * -1) + "h")
                                .minDocCount(0)
                                .extendedBounds( new ExtendedBounds(fromStart + offset, toEnd + offset )  )
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
    public void aggsTermsQuery_0() {
        long dateMil = TimeUnit.DAYS.toMillis(1);
        
        long toDate   = System.currentTimeMillis();
        long fromDate = System.currentTimeMillis() - dateMil *3;
        int timeZone = 8;
        
        long offset = TimeUnit.HOURS.toMillis(timeZone);
        
        long fromStart = getDateStartTime(fromDate, timeZone);
        long toEnd = getDateEndTime(toDate, timeZone);
        
        System.out.println(String.format("From %d to %d", fromStart, toEnd));
        
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                                                .filter(termQuery("orgId", 0L))
                                                .filter(termQuery("ownerId", 102L))
                                                .filter(rangeQuery("startDate").gte(fromStart).lte(toEnd));
        
        SearchResponse response = client.prepareSearch(INDEX_2)
                .setTypes(DOC)
                .setSize(0)
                .setQuery(queryBuilder)
                .addAggregation(
                        AggregationBuilders
                                .dateHistogram("date")
                                .field("startDate")
                                .dateHistogramInterval(DateHistogramInterval.DAY)
                                .offset(offset * -1)
                                .minDocCount(0)
                                .extendedBounds( new ExtendedBounds(fromStart + offset, toEnd + offset )  )
                 )
                .get();
        
//        Map<String, Aggregation> aggregationMap = response.getAggregations().asMap();
        InternalDateHistogram dateGroup = (InternalDateHistogram)response.getAggregations().get("date");
        dateGroup.getBuckets().forEach( item -> {
            System.out.println(item.getKeyAsString() + " : " + item.getDocCount());
        });
        
//        System.out.println(response.getAggregations().asMap());
        System.out.println(response);
    }
    
    private long getDateStartTime(long timestamp, int offset) {
        LocalDate localDate = LocalDate.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(offset)));
        LocalDateTime localDateTime = LocalDateTime.of(localDate, LocalTime.MIN);
        return Timestamp.valueOf(localDateTime).getTime();
    }
    
    private long getDateEndTime(long timestamp, int offset) {
        LocalDate localDate = LocalDate.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(offset)));
        LocalDateTime localDateTime = LocalDateTime.of(localDate, LocalTime.MAX);
        return Timestamp.valueOf(localDateTime).getTime();
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
