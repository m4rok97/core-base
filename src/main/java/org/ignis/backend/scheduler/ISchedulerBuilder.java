/*
 * Copyright (C) 2019 César Pomar
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
package org.ignis.backend.scheduler;

import org.ignis.backend.exception.ISchedulerException;


/**
 * @author César Pomar
 */
public class ISchedulerBuilder {

    public static IScheduler create(String type, String url) {
        type = type.toLowerCase();
        switch (type) {
            case IDockerScheduler.NAME:
                return new IDockerScheduler(url);
            case IAncorisScheduler.NAME:
                return new IAncorisScheduler(url);
            case IMarathonScheduler.NAME:
                return new IMarathonScheduler(url);
            case ISingularityScheduler.NAME:
                return new ISingularityScheduler(url);
            default:
                throw new ISchedulerException("Scheduler " + type + " not found");
        }
    }

}
