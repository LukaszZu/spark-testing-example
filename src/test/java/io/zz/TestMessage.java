/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.zz;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author zulk
 */
public class TestMessage {
    String name;
    String value;
    List<InMessage> inMessage = new ArrayList<>();

    public TestMessage() {
    }

    public List<InMessage> getInMessage() {
        return inMessage;
    }

    public void setInMessage(List<InMessage> inMessage) {
        this.inMessage = inMessage;
    }

    
    public TestMessage addMessage(InMessage message) {
        inMessage.add(message);
        return this;
    }
    
    public TestMessage(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "TestMessage{" + "name=" + name + ", value=" + value + ", inMessage=" + inMessage + '}';
    }
    
}
