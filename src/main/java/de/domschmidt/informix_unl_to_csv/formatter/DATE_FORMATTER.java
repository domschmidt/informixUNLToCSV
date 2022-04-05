package de.domschmidt.informix_unl_to_csv.formatter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class DATE_FORMATTER implements ICustomTableColumnFormatter {

    private final static DateTimeFormatter INPUT_PATTERN = DateTimeFormatter.ofPattern("dd.MM.yyyy");
    private final static DateTimeFormatter OUTPUT_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public String convert(final String contentRaw) {
        if (!contentRaw.trim().equals("")) {
            final LocalDate parsedDate = LocalDate.parse(contentRaw, INPUT_PATTERN);
            return parsedDate.format(OUTPUT_PATTERN);
        } else {
            return contentRaw;
        }
    }
}
