/**
 * 
 */
package com.zhang.eslearn.entity;

import java.io.Serializable;

import lombok.ToString;
import lombok.Value;

/**
 * @author zjie
 *
 */
@Value
@ToString
public class A3ViolationClientVo implements Serializable {

    private static final long serialVersionUID = 1L;
    
    long timestamp;
    long value;
}
