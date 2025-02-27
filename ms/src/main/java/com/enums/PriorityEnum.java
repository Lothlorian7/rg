package cn.bitoffer.msgcenter.enums;

public enum PriorityEnum {
    PRIORITY_LOW(1),
    PRIORITY_MIDDLE(2),
    PRIORITY_HIGH(3);

    private PriorityEnum(int priorty) {
        this.priorty = priorty;
    }
    private int priorty;

    public int getPriorty() {
        return this.priorty;
    }

    public static String GetPriorityStr(int priorty) {
        if (priorty == PRIORITY_LOW.getPriorty()){
            return "low";
        }else if (priorty == PRIORITY_MIDDLE.getPriorty()){
            return "middle";
        }else if (priorty == PRIORITY_HIGH.getPriorty()){
            return "high";
        }
        return "low";
    }
}
