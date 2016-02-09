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

/**
 * Created by gpalos on 21. 1. 2016.
 */
public class SapFilter {
    private String byName; // same as byUid

    private String byNameContains;

    public String getByNameContains() {
        return byNameContains;
    }

    public void setByNameContains(String byNameContains) {
        this.byNameContains = byNameContains;
    }

    public String getByName() {
        return byName;
    }

    public void setByName(String byName) {
        this.byName = byName;
    }

    @Override
    public String toString() {
        return "SapFilter{" +
                "byName='" + byName + '\'' +
                ", byNameContains='" + byNameContains + '\'' +
                '}';
    }
}
