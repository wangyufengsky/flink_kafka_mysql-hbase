package entity;

import lombok.*;

import java.math.BigDecimal;

@ToString @AllArgsConstructor @Builder
public class SnapshotBean {
    @Getter @Setter
    private Long cst_no;
    @Getter @Setter
    private Long create_date;
    @Getter @Setter
    private String busi_type;
    @Getter @Setter
    private BigDecimal total_bal_val;
    @Getter @Setter
    private BigDecimal total_profit_val;


}
