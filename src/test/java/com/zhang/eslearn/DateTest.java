/**
 * 
 */
package com.zhang.eslearn;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author zjie
 *
 */
@RunWith(JUnit4.class)
public class DateTest {

    @Test
    public void test() {
        long dateMil = TimeUnit.DAYS.toMillis(1);
        long toDate = System.currentTimeMillis();
        long fromDate = toDate - TimeUnit.DAYS.toMillis(1);
        int timeZone = 8;
        
        System.out.println(TimeUnit.HOURS.toMillis(-8));
        
        System.out.println(String.format("From %d to %d", fromDate, toDate));
        
        System.out.println(String.format("From %d to %d", 
                fromDate - fromDate%dateMil - TimeUnit.HOURS.toMillis(timeZone), 
                toDate - toDate%dateMil + TimeUnit.DAYS.toMillis(1) - TimeUnit.HOURS.toMillis(timeZone) -1
                ));
    }
}
