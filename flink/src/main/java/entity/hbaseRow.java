package entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class hbaseRow {
    private String rowKey;
    private String family;
    private String colunm;
    private BigDecimal value;
    private String onlyOne;

    public hbaseRow(String rowKey,String family,String colunm,BigDecimal value) {
        this.rowKey = rowKey;
        this.family = family;
        this.colunm =colunm;
        this.value = value;
    }

    public String getOnlyOne() {
        return rowKey+colunm;
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getColunm() {
        return colunm;
    }

    public void setColunm(String colunm) {
        this.colunm = colunm;
    }

    public BigDecimal getValue() {
        return value;
    }

    public void setValue(BigDecimal value) {
        this.value = value;
    }
}
