/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.terracottatech.frs;

import java.io.Closeable;

/**
 *
 * @author mscott
 */
public interface Disposable extends Closeable {
    void dispose();
}
