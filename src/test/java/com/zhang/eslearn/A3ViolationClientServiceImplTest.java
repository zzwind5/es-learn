package com.zhang.eslearn;

import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.zhang.eslearn.service.A3ViolationClientServiceImpl;

@RunWith(SpringRunner.class)
@SpringBootTest
public class A3ViolationClientServiceImplTest {

    @Autowired private A3ViolationClientServiceImpl service;
    
    
    @Test
    public void service() {
        var to = System.currentTimeMillis();
        var from = to - TimeUnit.DAYS.toMillis(2);
        var result = service.getViolationClientStat(from, to, -8);
        result.forEach(System.out::println);
    }
}
