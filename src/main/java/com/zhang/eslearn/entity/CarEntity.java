package com.zhang.eslearn.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import lombok.Builder;
import lombok.Data;

@Document(indexName="cars", type="transactions")
@Data
@Builder(toBuilder=true)
public class CarEntity {
    
    @Id
    private String id;
    
    private int price;
    private String color;
    private String make;
    private String sold;
}
