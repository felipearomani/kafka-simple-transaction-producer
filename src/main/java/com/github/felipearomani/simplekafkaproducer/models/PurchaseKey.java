package com.github.felipearomani.simplekafkaproducer.models;

import java.io.Serializable;
import java.util.Date;
import java.util.Objects;

public class PurchaseKey implements Serializable {
    private String costumerId;
    private Date transactionDate;

    public PurchaseKey(String costumerId, Date transactionDate) {
        this.costumerId = costumerId;
        this.transactionDate = transactionDate;
    }

    public String getCostumerId() {
        return costumerId;
    }

    public Date getTransactionDate() {
        return transactionDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PurchaseKey that = (PurchaseKey) o;
        return Objects.equals(costumerId, that.costumerId) &&
                Objects.equals(transactionDate, that.transactionDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(costumerId, transactionDate);
    }
}
