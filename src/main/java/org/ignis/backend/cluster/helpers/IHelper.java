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
package org.ignis.backend.cluster.helpers;

import org.ignis.backend.properties.IProperties;
import org.ignis.rpc.ISource;

/**
 * @author César Pomar
 */
public abstract class IHelper {

    protected final IProperties properties;

    public IHelper(IProperties properties) {
        this.properties = properties;
    }

    protected abstract String getName();

    protected String log() {
        return getName() + ": ";
    }

    protected String srcToString(ISource src) {
        StringBuilder result = new StringBuilder("ISource(");
        boolean flag = false;
        if (src.isSetObj()) {
            if (src.getObj().isSetBytes()) {
                result.append("[..binary..]");
                flag = true;
            } else if (src.getObj().isSetName()) {
                result.append('"');
                result.append(src.getObj().getName());
                result.append('"');
                flag = true;
            }
        }
        if (src.isSetParams() && !src.getParams().isEmpty()) {
            if (flag) {
                result.append(", ");
            }
            result.append("args=[");
            for (String var : src.getParams().keySet()) {
                result.append(var);
                result.append(", ");
            }
            result.delete(result.length() - 2, result.length());
        }

        result.append(')');
        return result.toString();
    }

}
