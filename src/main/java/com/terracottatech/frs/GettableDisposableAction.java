/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs;

import java.nio.ByteBuffer;

/**
 *
 * @author mscott
 */
public class GettableDisposableAction extends DisposableAction implements GettableAction {

    public GettableDisposableAction(GettableAction a, Disposable dispose) {
        super(a, dispose);
    }
    
    protected GettableAction getGettableAction() {
        return (GettableAction)super.getAction();
    }

    @Override
    public long getLsn() {
        return getGettableAction().getLsn();
    }

    @Override
    public ByteBuffer getIdentifier() {
        return getGettableAction().getIdentifier();
    }

    @Override
    public ByteBuffer getKey() {
        return getGettableAction().getKey();
    }

    @Override
    public ByteBuffer getValue() {
        return getGettableAction().getValue();
    }
}
