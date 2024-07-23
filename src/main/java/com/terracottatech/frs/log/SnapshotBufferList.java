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
package com.terracottatech.frs.log;

import com.terracottatech.frs.SnapshotRequest;
import com.terracottatech.frs.Snapshot;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author mscott
 */
public class SnapshotBufferList extends BufferListWrapper implements SnapshotRequest {
    
    List<SnapshotRequest> holder = new LinkedList<SnapshotRequest>();

    public SnapshotBufferList(List<ByteBuffer> base, List<SnapshotRequest> holder) {
      super(base);
        this.holder = holder;
    }

    @Override
    public void setSnapshot(Snapshot snap) {
        for (SnapshotRequest req : holder ) {
            req.setSnapshot(snap);
        }
    }

}
