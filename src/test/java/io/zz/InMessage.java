/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import java.io.Serializable;

/**
 *
 * @author zulk
 */
public class InMessage implements Serializable{
    String name;
    String name2;

    public InMessage(String name) {
        this.name = name;
    }

    public InMessage() {
    }

    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "InMessage{" + "name=" + name + '}';
    }

    public String getName2() {
        return name2;
    }

    public void setName2(String name2) {
        this.name2 = name2;
    }
 
    
    
}
