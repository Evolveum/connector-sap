/*
 * Copyright (c) 2010-2016 Evolveum
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

package com.evolveum.polygon.connector.sap;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;

/**
 * Created by gpalos on 4. 7. 2016.
 */
public class SapBasicFilterTranslator extends AbstractFilterTranslator<SapFilter> {
    private static final Log LOG = Log.getLog(SapBasicFilterTranslator.class);

    @Override
    protected SapFilter createEqualsExpression(EqualsFilter filter, boolean not) {
        LOG.ok("createEqualsExpression, filter: {0}, not: {1}", filter, not);

        if (not) {
            return null;            // not supported attribute
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        // filter by NAME is the same as by UID
        if (Name.NAME.equals(attr.getName()) || Uid.NAME.equals(attr.getName())) {
            if (attr.getValue() != null && attr.getValue().get(0) != null) {
                SapFilter lf = new SapFilter(String.valueOf(attr.getValue().get(0)));
                return lf;
            }
        }

        return null;            // not supported attribute
    }

}
