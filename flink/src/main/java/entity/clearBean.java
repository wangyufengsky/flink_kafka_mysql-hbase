package entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
public class clearBean {
    @Setter @Getter
    private String event_no;
    @Getter
    private int cst_no;
    @Setter @Getter
    private BigDecimal clear_total;
    @Setter @Getter
    private String date;
    @Setter @Getter
    private String time;
    @Setter @Getter
    private String table;
    @Setter @Getter
    private String dataBase;

    @Setter @Getter
    private String busi_code;

    public void setCst_no(int cst_no) {
        this.cst_no = cst_no;
        this.table = String.valueOf(cst_no%10);
        this.dataBase=String.valueOf((cst_no/10000)%28);
    }

}
