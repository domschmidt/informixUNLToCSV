package de.domschmidt.informix_unl_to_csv.defaults;

public class StaticStringDefault implements ITableDefaultValue {

    private final String defaultValue;

    public StaticStringDefault(final String defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public String get() {
        return this.defaultValue;
    }
}
