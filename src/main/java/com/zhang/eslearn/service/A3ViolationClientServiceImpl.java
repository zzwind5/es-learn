/**
 * 
 */
package com.zhang.eslearn.service;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.elasticsearch.search.aggregations.bucket.histogram.InternalDateHistogram;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import com.zhang.eslearn.entity.A3ViolationClientVo;

/**
 * @author zjie
 *
 */
@Service
public class A3ViolationClientServiceImpl {
    
    @Autowired private ElasticsearchTemplate esTemplate;

    public List<A3ViolationClientVo> getViolationClientStat(final long from, final long to, final int timeOffSet) {
        
        var fromDateStart = getDateStartTime(from, timeOffSet);
        var toDateEnd = getDateEndTime(to, timeOffSet);
        var timeOffSetMil = TimeUnit.HOURS.toMillis(timeOffSet);
        
        System.out.println("From " + fromDateStart + " to " + toDateEnd);
        
        var queryBuilder = QueryBuilders
                                .boolQuery()
                                .filter( termQuery("ownerId", 102L) )
                                .filter( termQuery("orgId", 0l) )
                                .filter( rangeQuery("startDate").gte(fromDateStart).lte(toDateEnd) );
        
        var aggBuilder   = AggregationBuilders
                                .dateHistogram("date")
                                .field("startDate")
                                .dateHistogramInterval(DateHistogramInterval.DAY)
                                .offset(timeOffSetMil * -1)
                                .minDocCount(0)
                                .extendedBounds( new ExtendedBounds(fromDateStart + timeOffSetMil, toDateEnd + timeOffSetMil ) );
        
        var query        = new NativeSearchQueryBuilder()
                                .withIndices("a3-reporting-violation-client-*")
                                .withTypes("a3-reporting-violation-client")
                                .withQuery(queryBuilder)
                                .addAggregation(aggBuilder)
                                .build();
        
        return esTemplate.query(query, response -> {
            System.out.println(response);
            var resList = new ArrayList<A3ViolationClientVo>();
            
            InternalDateHistogram dateGroup = (InternalDateHistogram)response.getAggregations().get("date");
            dateGroup.getBuckets().forEach( item -> {
                System.out.println(item.getKey());
                System.out.println(item.getKeyAsString());
                resList.add( new A3ViolationClientVo( ((DateTime)item.getKey()).getMillis(), item.getDocCount()) );
            });
            return resList;
        });
    }
    
    private long getDateStartTime(final long timestamp, final int offset) {
        var oneDate = TimeUnit.DAYS.toMillis(1);
        var timeOffset = TimeUnit.HOURS.toMillis(offset);
        System.out.println(timeOffset);
        System.out.println(timestamp - timestamp%oneDate);
        
        return timestamp - timestamp%oneDate - timeOffset;
                
//        LocalDate localDate = LocalDate.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(offset)));
//        LocalDateTime localDateTime = LocalDateTime.of(localDate, LocalTime.MIN);
//        return Timestamp.valueOf(localDateTime).getTime();
    }
    
    private long getDateEndTime(final long timestamp, final int offset) {
          return getDateStartTime(timestamp, offset) + TimeUnit.DAYS.toMillis(1) -1;
//        LocalDate localDate = LocalDate.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(offset)));
//        LocalDateTime localDateTime = LocalDateTime.of(localDate, LocalTime.MAX);
//        return Timestamp.valueOf(localDateTime).getTime();
    }

}
