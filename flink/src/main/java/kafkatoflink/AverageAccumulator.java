package kafkatoflink;

import java.math.BigDecimal;

public class AverageAccumulator {

    String key;
    long count;
    BigDecimal sum;
    String certNo;
    String all;
    BigDecimal money;
    String date;
    String type;
    public AverageAccumulator(){
        key="";
        count=0L;
        sum=new BigDecimal(0);
        certNo="";
        all="";
        money=new BigDecimal(0);
        date="";
        type="";
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public BigDecimal getSum() {
        return sum;
    }

    public void setSum(BigDecimal sum) {
        this.sum = sum;
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
