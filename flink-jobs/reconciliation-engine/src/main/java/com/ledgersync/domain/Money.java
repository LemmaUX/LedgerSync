package com.ledgersync.domain;

/**
 * Money - Represents a monetary value with currency.
 */
public class Money {
    private Long value;
    private String currency;

    public Money() {}

    public Money(Long value, String currency) {
        this.value = value;
        this.currency = currency;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    @Override
    public String toString() {
        return "Money{value=" + value + ", currency='" + currency + "'}";
    }
}
