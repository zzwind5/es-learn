package com.zhang.eslearn;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsLearnApplicationTests {
    
    private static final String INDEX = "cars";
    private static final String DOC = "transactions";

    @Autowired
    private TransportClient client;

	@Test
	public void contextLoads() {
	}
	
	@Test
	public void aggsTermsQuery() {
	    SearchResponse response = client.prepareSearch(INDEX)
	            .setTypes(DOC)
	            .setSize(0)
	            .addAggregation(
	                    AggregationBuilders.terms("popular_colors")
	                                        .field("color")
	             )
	            .get();
	    System.out.println(response.getAggregations());
	}
	
	@Test
    public void aggsTermsQuery_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders.terms("popular_colors")
                                            .field("color")
                                            .subAggregation(
                                                    AggregationBuilders.avg("avg_price")
                                                                       .field("price")
                                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_2() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders.terms("popular_colors")
                                            .field("color")
                                            .subAggregation(
                                                    AggregationBuilders.avg("avg_price")
                                                                       .field("price")
                                             )
                                             .subAggregation(
                                                     AggregationBuilders.terms("make")
                                                                         .field("make")
                                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_3() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders.terms("popular_colors")
                                            .field("color")
                                            .subAggregation(
                                                    AggregationBuilders.avg("avg_price")
                                                                       .field("price")
                                             )
                                             .subAggregation(
                                                     AggregationBuilders.terms("make")
                                                                         .field("make")
                                                                         .subAggregation(
                                                                                 AggregationBuilders.min("min_price")
                                                                                                     .field("price")
                                                                          )
                                                                         .subAggregation(
                                                                                 AggregationBuilders.max("max_price")
                                                                                                     .field("price")
                                                                          )
                                             )
                 )
                .get();
        System.out.println(response);
    }
	
	
	@Test
    public void aggsTermsQuery_2_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders
                            .histogram("price")
                            .field("price")
                            .interval(20000)
                            .subAggregation(
                                    AggregationBuilders
                                        .sum("revenue")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_2_2() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders
                            .terms("make")
                            .field("make")
                            .subAggregation(
                                    AggregationBuilders
                                        .extendedStats("revenue")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_3_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders
                            .dateHistogram("sales")
                            .field("sold")
                            .format("yyyy-MM-dd")
                            .dateHistogramInterval(DateHistogramInterval.MONTH)
                            .subAggregation(
                                    AggregationBuilders
                                        .sum("all_sales")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_3_2() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders
                            .dateHistogram("sales")
                            .field("sold")
                            .format("yyyy-MM-dd")
                            .dateHistogramInterval(DateHistogramInterval.MONTH)
                            .minDocCount(0)
                            .extendedBounds( new ExtendedBounds("2014-01-01", "2014-12-31"))
                            .subAggregation(
                                    AggregationBuilders
                                        .sum("all_sales")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_4_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .addAggregation(
                        AggregationBuilders
                            .dateHistogram("sales")
                            .field("sold")
                            .format("yyyy-MM-dd")
                            .dateHistogramInterval(DateHistogramInterval.QUARTER)
                            .minDocCount(0)
                            .extendedBounds( new ExtendedBounds("2014-01-01", "2014-12-31"))
                            .subAggregation(
                                    AggregationBuilders
                                        .terms("per_make_sum")
                                        .field("make")
                                        .subAggregation(
                                                AggregationBuilders
                                                        .sum("sum_price")
                                                        .field("price")
                                         )
                             )
                            .subAggregation(
                                    AggregationBuilders
                                        .sum("total_sum")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_5_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .setQuery(
                        QueryBuilders.matchQuery("make", "honda")
                 )
                .addAggregation(
                        AggregationBuilders
                            .terms("colors")
                            .field("color")
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_5_2() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .setQuery(
                        QueryBuilders.matchQuery("make", "honda")
                 )
                .addAggregation(
                        AggregationBuilders
                            .avg("single_avg_price")
                            .field("price")
                 )
                .addAggregation(
                        AggregationBuilders
                            .global("avg_price_name")
                            .subAggregation(
                                    AggregationBuilders
                                        .avg("avg_price")
                                        .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
	
	@Test
    public void aggsTermsQuery_6_1() {
        SearchResponse response = client.prepareSearch(INDEX)
                .setTypes(DOC)
                .setSize(0)
                .setQuery(
                        QueryBuilders.matchQuery("make", "honda")
                 )
                .addAggregation(
                        AggregationBuilders
                            .filter("recent_sales", QueryBuilders.rangeQuery("sold").from("now-1M"))
                            .subAggregation(
                                    AggregationBuilders
                                            .avg("single_avg_price")
                                            .field("price")
                             )
                 )
                .get();
        System.out.println(response);
    }
}
