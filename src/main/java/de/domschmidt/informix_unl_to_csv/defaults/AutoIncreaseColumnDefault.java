package de.domschmidt.informix_unl_to_csv.defaults;

public class AutoIncreaseColumnDefault implements ITableDefaultValue {

    private Integer currentValue;

    public AutoIncreaseColumnDefault(final Integer startValue) {
        this.currentValue = startValue;
    }

    @Override
    public String get() {
        return (this.currentValue ++).toString();
    }
}
