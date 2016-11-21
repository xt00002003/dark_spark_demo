package com.dark.channel.util;

import org.joda.time.*;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/**
 * DateTime Utils
 */
public class DateTimeUtils {
    private static final DateTimeZone dateTimeZone = DateTimeZone.forID("Asia/Shanghai");

    private static final DateTimeFormatter dateFormatter =
        DateTimeFormat.forPattern("yyyy-MM-dd").withZone(dateTimeZone);

    private static final DateTimeZone esTimeZone = DateTimeZone.UTC;

    //零时区格式化
    private static final DateTimeFormatter utcDateFormatter =
        DateTimeFormat.forPattern("yyyy-MM-dd").withZone(esTimeZone);


    private static final DateTimeFormatter esFormatter = ISODateTimeFormat.dateTime().withZoneUTC();

    public static DateTime toDateTime(DateTime time) {
        return time.withZone(dateTimeZone);
    }

    public static DateTime fromDateString(String date) {
        return dateFormatter.parseDateTime(date);
    }

    public static String toDateString(DateTime time) {
        return dateFormatter.print(time.getMillis());
    }

    public static DateTime fromUtcDateString(String date){
        return utcDateFormatter.parseDateTime(date);
    }

    public static DateTime toEsTime(DateTime time) {
        return time.withZone(esTimeZone);
    }

    public static DateTime fromEsTimeString(String esTimeString) {
        return esFormatter.parseDateTime(esTimeString);
    }

    public static String toEsTimeString(DateTime time) {
        return toEsTimeString(time.getMillis());
    }

    public static String toEsTimeString(Long millis) {
        return esFormatter.print(millis);
    }

    public static int formatSeconds(long duration) {
        if (duration < 0) {
            duration = 0;
        }
        PeriodType periodType = PeriodType.forFields(
            new DurationFieldType[] {
                DurationFieldType.days(),
                DurationFieldType.hours(),
                DurationFieldType.minutes(),
                DurationFieldType.seconds(),
            });
        Duration d = Duration.standardSeconds(duration);
        Period period = d.toPeriod(periodType, GregorianChronology.getInstance());
        period = period.normalizedStandard(periodType);
        int days = period.getDays();
        int hours = period.getHours();
        final int seconds = 3600;
        if (days > 0) {
            hours = (days + 1) * 24;
        } else if (hours > 0) {
            hours = (hours + 1);
        } else {
            hours = 1;
        }
        return hours * seconds;
    }

}
