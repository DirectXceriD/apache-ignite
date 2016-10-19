/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import _ from 'lodash';
import StringBuilder from './StringBuilder';

const dateOptions = {
    month: '2-digit',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
};

/**
 * Properties generation entry point.
 */
export default class PropertiesGenerator {
    _collectProperties(bean) {
        const lines = [];

        _.forEach(bean.properties, (prop) => {
            switch (prop.clsName) {
                case 'DATA_SOURCE':
                    lines.push(...this._collectProperties(prop.value));
                    lines.push('');

                    break;
                case 'BEAN':
                    lines.push(...this._collectProperties(prop.value));

                    break;
                case 'PROPERTY':
                case 'PROPERTY_CHAR':
                    lines.push(`${prop.value}=${prop.hint}`);

                    break;
                case 'ARRAY':
                case 'COLLECTION':
                    _.forEach(prop.items, (item) => {
                        const itemLines = this._collectProperties(item);

                        if (_.intersection(lines, itemLines).length !== itemLines.length)
                            lines.push(...this._collectProperties(item));
                    });

                    break;
                default:
            }
        });

        return lines;
    }

    generate(cfg) {
        const lines = this._collectProperties(cfg);

        if (_.isEmpty(lines))
            return null;

        const sb = new StringBuilder();

        sb.append(`# This file was generated by Ignite Web Console (${new Date().toLocaleString('en-US', dateOptions)})`);

        _.forEach(lines, (line) => sb.append(line));

        return sb.asString();
    }
}
