package entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;

@ToString
public class amountBean {
    @Setter @Getter
    private String event_no;
    @Setter @Getter
    private int cst_no;
    @Setter @Getter
    private BigDecimal clear_amount;
    @Setter @Getter
    private String date;
    @Setter @Getter
    private String time;
    @Setter @Getter
    private String busi_code;
}
