package de.domschmidt.informix_unl_to_csv.formatter;

import java.time.MonthDay;
import java.time.format.DateTimeFormatter;

public class MONTH_DAY_TO_DATE_FORMATTER implements ICustomTableColumnFormatter {

    private final static DateTimeFormatter INPUT_PATTERN = DateTimeFormatter.ofPattern("MM-dd");
    private final static DateTimeFormatter OUTPUT_PATTERN = DateTimeFormatter.ofPattern("'1970'-MM-dd");

    @Override
    public String convert(final String contentRaw) {
        if (!contentRaw.trim().equals("")) {
            final MonthDay parsedDate = MonthDay.parse(contentRaw, INPUT_PATTERN);
            return parsedDate.format(OUTPUT_PATTERN);
        } else {
            return contentRaw;
        }
    }
}
