/*
 * Copyright (C) 2018 
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.ignis.backend.cluster.helpers.data;

import java.util.ArrayList;
import java.util.List;
import org.ignis.backend.cluster.IData;
import org.ignis.backend.cluster.ISplit;
import org.ignis.backend.cluster.tasks.IBarrier;
import org.ignis.backend.cluster.tasks.executor.IReduceByKeyTask;
import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISourceFunction;

/**
 *
 * @author César Pomar
 */
public class IDataReduceHelper extends IDataHelper {

    public IDataReduceHelper(IData data, IProperties properties) {
        super(data, properties);

    }

    public IData reduceByKey(ISourceFunction function) {
        IBarrier barrier = new IBarrier(data.getSplitSize());
        IReduceByKeyTask.KeyShared keyShared = new IReduceByKeyTask.KeyShared();
        List<ISplit> result = new ArrayList<>();
        for (ISplit split : data.getSplits()) {
            result.add(new ISplit(split.getExecutor(), new IReduceByKeyTask(split.getExecutor(), function, barrier,
                    keyShared, data.getLock(), split.getTask())));
        }
        return new IData(data.getJob().getDataSize(), data.getJob(), result);
    }

}
