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
package com.terracottatech.frs;

import com.terracottatech.frs.action.Action;
import com.terracottatech.frs.recovery.AbstractFilter;
import com.terracottatech.frs.recovery.Filter;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author tim
 */
public class DeleteFilter extends AbstractFilter<Action> {
  private final Set<ByteBuffer> deleted = new HashSet<ByteBuffer>();

  public DeleteFilter(Filter<Action> nextFilter) {
    super(nextFilter);
  }

  @Override
  public boolean filter(Action element, long lsn, boolean filtered) {
    if (element instanceof DeleteAction) {
      deleted.add(detach(((DeleteAction) element).getId()));
      return delegate(element, lsn, true);
    } else if (element instanceof GettableAction && deleted.contains(((GettableAction) element).getIdentifier())) {
      return delegate(element, lsn, true);
    } else {
      return delegate(element, lsn, filtered);
    }
  }
  
  private static ByteBuffer detach(ByteBuffer buffer) {
    byte[] alloc = new byte[buffer.remaining()];
    buffer.mark();
    buffer.get(alloc);
    buffer.reset();
    return ByteBuffer.wrap(alloc);
  }
}
