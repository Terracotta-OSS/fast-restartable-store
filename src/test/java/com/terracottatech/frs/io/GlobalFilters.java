/*
 * All content copyright (c) 2012 Terracotta, Inc., except as may otherwise
 * be noted in a separate copyright notice. All rights reserved.
 */
package com.terracottatech.frs.io;

/**
 *
 * @author mscott
 */
public class GlobalFilters {
    
    private static BufferFilter filters;
    
    public static BufferFilter getFilters() {
        return filters;
    }
    
    public static void addFilter(BufferFilter f) {
        if ( filters == null ) filters = f;
        else filters.add(f);
    }
}
