/**
 * 
 */
package com.zhang.eslearn;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
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
    
    @Test
    public void test_1() {
        long now = System.currentTimeMillis();
        System.out.println(now);
        System.out.println(Instant.ofEpochMilli(now).atOffset(ZoneOffset.ofHours(-8)));
        
        LocalDateTime localDateTime = LocalDateTime.of(LocalDate.now(), LocalTime.MIN);
        
        System.out.println(localDateTime);
        System.out.println(localDateTime.toLocalDate());
        Timestamp.valueOf(LocalDateTime.now());
        System.out.println(Timestamp.valueOf(localDateTime).getTime());
        
//        LocalDate.ofInstant(Instant.ofEpochMilli(now), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(8)))
        System.out.println( getDateStartTime(now, -8) );
        System.out.println( getDateEndTime(now, -8) );
        
//        Instant instant = Instant.ofEpochMilli(now);
//        ZoneOffset.ofHours(8);
//        ZoneId zone = ZoneId.ofOffset(prefix, offset)
//        return LocalDateTime.ofInstant(instant, zone);
//        
//        
//        Instant.ofEpochMilli(now).atOffset(offset)
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
}
