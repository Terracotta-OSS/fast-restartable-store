/*
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.terracottatech.frs.util;

import java.util.*;

/**
 *
 * @author mscott
 */
public class LongBitSet implements Set<Long> {
    
    HashMap<Long,BitSet> master = new HashMap<Long,BitSet>();
    private final int segmentation = Integer.MAX_VALUE;
    enum OP {FIND,CREATE,REMOVE};
    
    private BitSet divide(long value, OP op) {
        long slot = (value / segmentation);
        if ( op == OP.REMOVE ) {
            return master.remove(slot);
        }
        BitSet set = master.get(slot);
        if ( set == null && op == OP.CREATE ) {
            set = new BitSet();
            master.put(slot, set);
        }
        
        return set;
    }
    
    private int offset(long value) {
        return (int)(value % segmentation);
    }

    @Override
    public boolean add(Long e) {
        divide(e,OP.CREATE).set(offset(e));
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends Long> clctn) {
        for ( Long e : clctn ) {
            add(e);
        }
        return true;
    }

    @Override
    public void clear() {
        master.clear();
    }

    @Override
    public boolean contains(Object o) {
        if ( o instanceof Long ) {
            Long e = (Long)o;
            BitSet set = divide(e,OP.FIND);
            if ( set == null ) return false;
            return set.get(offset(e));
        }   
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> clctn) {
        for ( Object e : clctn ) {
            if ( !contains(e) ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isEmpty() {
        return master.isEmpty();
    }

    @Override
    public Iterator<Long> iterator() {
        //  NOT IMPLEMENTED 
        return null;
    }

    @Override
    public boolean remove(Object o) {
        if ( o instanceof Long ) {
            Long e = (Long)o;
            BitSet set = divide(e,OP.FIND);
            if ( set == null ) return false;
            if ( set.get(offset(e)) ) {
                set.clear(offset(e));
                if ( set.isEmpty() ) divide(e,OP.REMOVE);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> clctn) {
        for ( Object e : clctn ) {
            if ( !remove(e) ) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> clctn) {
        //  NOT IMPLEMENTED 
        return false;
    }

    @Override
    public int size() {
        int count = 0;
        for ( BitSet b : master.values() ) {
            count += b.cardinality();
        }
        return count;
    }

    @Override
    public Object[] toArray() {
        //  NOT IMPLEMENTED 
        return null;
    }

    @Override
    public <T> T[] toArray(T[] ts) {
        //  NOT IMPLEMENTED 
        return null;
    }
    
}
