package kr.ac.korea;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ideapad on 2015-10-11.
 */
class LineItem implements WritableComparable<LineItem> {
    private long orderkey;
    private long linenumber;
    private double quantity;
    private double discount;
    private double tax;


    public void set(long orderkey, long linenumber, double quantity, double discount, double tax){
        this.orderkey   = orderkey;
        this.linenumber = linenumber;
        this.quantity   = quantity;
        this.discount   = discount;
        this.tax        = tax;
    }
    public void write(DataOutput output) throws IOException {
        output.writeLong(orderkey);
        output.writeLong(linenumber);
        output.writeDouble(quantity);
        output.writeDouble(discount);
        output.writeDouble(tax);
    }

    public void readFields(DataInput input) throws IOException {
        orderkey    = input.readLong();
        linenumber  = input.readLong();
        quantity    = input.readDouble();
        discount    = input.readDouble();
        tax         = input.readDouble();
    }

    public int compareTo(LineItem item) {
       return 0;
    }

    public long getOrderkey() {
        return orderkey;
    }

    public void setOrderkey(long orderkey) {
        this.orderkey = orderkey;
    }

    public long getLinenumber() {
        return linenumber;
    }

    public void setLinenumber(long linenumber) {
        this.linenumber = linenumber;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public double getDiscount() {
        return discount;
    }

    public void setDiscount(double discount) {
        this.discount = discount;
    }

    public double getTax() {
        return tax;
    }

    public void setTax(double tax) {
        this.tax = tax;
    }


}
