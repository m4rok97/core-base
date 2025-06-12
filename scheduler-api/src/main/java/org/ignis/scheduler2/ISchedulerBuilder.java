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
package org.ignis.scheduler2;


import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author César Pomar
 */
public final class ISchedulerBuilder {

    private ISchedulerBuilder() {
    }

    private static String capitalize(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    @SuppressWarnings({"unchecked"})
    public static IScheduler create(String type, String url) {
        Class<? extends IScheduler> found;
        if (type.indexOf('.') == -1) {
            type = "org.ignis.scheduler." + Arrays.stream(type.split("_"))
                    .map(ISchedulerBuilder::capitalize).collect(Collectors.joining());
        }

        try {
            found = (Class<? extends IScheduler>) Class.forName(type);
        } catch (ClassNotFoundException ex) {
            throw new ISchedulerException("Scheduler '" + type + "' not found", ex);
        } catch (ClassCastException ex) {
            throw new ISchedulerException("Scheduler '" + type + "' is not a valid IScheduler", ex);
        } catch (Throwable ex) {
            throw new ISchedulerException("Scheduler '" + type + "' can not be loaded", ex);
        }

        try {
            return found.getDeclaredConstructor(String.class).newInstance(url);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException |
                 NoSuchMethodException ex) {
            throw new ISchedulerException("Scheduler '" + type + "' can not be instantiated", ex);
        }
    }

}
