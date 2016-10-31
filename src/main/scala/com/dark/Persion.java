package com.dark;

import java.io.Serializable;

/**
 * Created by darkxue on 21/10/16.
 */
public class Persion implements Serializable {
    private int id;

    public Persion(int id){
        this.id=id;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
