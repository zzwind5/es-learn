package com.zhang.eslearn;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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
}
