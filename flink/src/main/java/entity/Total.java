package entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class Total {

    private String certNo;
    private BigDecimal totalAmt;
    private String date;
    private String type;
    private Timestamp time;

    public Total(String certNo,String date,String type,BigDecimal totalAmt) {
        this.certNo = certNo;
        this.date = date;
        this.type =type;
        this.totalAmt = totalAmt;
    }

    public String getRowKey(){
        return new StringBuffer(certNo).reverse()+date;
    }

    public String getOnlyone(){
        return new StringBuffer(certNo).reverse()+date+type;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public String getColum(){
        return type+time.toString();
    }


    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
    }

    public BigDecimal getTotalAmt() {
        return totalAmt;
    }

    public void setTotalAmt(BigDecimal totalAmt) {
        this.totalAmt = totalAmt;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
