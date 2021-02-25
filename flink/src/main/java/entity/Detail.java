package entity;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class Detail {

    private String certNo;
    private String all;
    private BigDecimal money;
    private String date;
    private String name;
    private String type;
    private Timestamp time;
    private BigDecimal totalAmt;


    public String getRowKey(){
        return new StringBuffer(certNo).reverse()+date;
    }

    public String getOnlyone(){
        return new StringBuffer(certNo).reverse()+date+type;
    }

    public String getColum(){
        return type+time.toString();
    }

    public BigDecimal getTotalAmt() {
        return totalAmt;
    }

    public void setTotalAmt(BigDecimal totalAmt) {
        this.totalAmt = totalAmt;
    }

    public Timestamp getTime() {
        return time;
    }

    public void setTime(Timestamp time) {
        this.time = time;
    }

    public String getCertNo() {
        return certNo;
    }

    public void setCertNo(String certNo) {
        this.certNo = certNo;
    }

    public String getAll() {
        return all;
    }

    public void setAll(String all) {
        this.all = all;
    }

    public BigDecimal getMoney() {
        return money;
    }

    public void setMoney(BigDecimal money) {
        this.money = money;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
