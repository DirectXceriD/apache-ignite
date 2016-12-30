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

'use strict';

// Fire me up!

/**
 * Module interaction with agents.
 */
module.exports = {
    implements: 'agent-socket',
    inject: ['require(lodash)']
};

/**
 * Helper class to contract REST command.
 */
class Command {
    /**
     * @param {Boolean} demo Is need run command on demo node.
     * @param {String} name Command name.
     */
    constructor(demo, name) {
        this._demo = demo;

        /**
         * Command name.
         * @type {String}
         */
        this._name = name;

        /**
         * Command parameters.
         * @type {Array.<Object.<String, String>>}
         */
        this._params = [];

        this._paramsLastIdx = 0;
    }

    /**
     * Add parameter to command.
     * @param {Object} value Parameter value.
     * @returns {Command}
     */
    addParam(value) {
        this._params.push({key: `p${this._paramsLastIdx++}`, value});

        return this;
    }

    /**
     * Add parameter to command.
     * @param {String} key Parameter key.
     * @param {Object} value Parameter value.
     * @returns {Command}
     */
    addNamedParam(key, value) {
        this._params.push({key, value});

        return this;
    }
}

/**
 * @param _
 * @returns {AgentSocket}
 */
module.exports.factory = function(_) {
    /**
     * Connected agent descriptor.
     */
    class AgentSocket {
        /**
         * @param {socketIo.Socket} socket - AgentSocket socket for interaction.
         */
        constructor(socket) {
            /**
             * AgentSocket socket for interaction.
             *
             * @type {socketIo.Socket}
             */
            this.socket = socket;
        }

        /**
         * Send event to agent.
         *
         * @this {AgentSocket}
         * @param {String} event Command name.
         * @param {Array.<Object>} args - Transmitted arguments.
         * @param {Function} [callback] on finish
         */
        _emit(event, args, callback) {
            if (!this.socket.connected) {
                if (callback)
                    callback('org.apache.ignite.agent.AgentException: Connection is closed');

                return;
            }

            this.socket.emit(event, ...args, callback);
        }

        /**
         * Send event to agent.
         *
         * @param {String} event - Event name.
         * @param {Array.<Object>?} args - Transmitted arguments.
         * @returns {Promise}
         */
        emitEvent(event, ...args) {
            return new Promise((resolve, reject) =>
                this._emit(event, args, (error, res) => {
                    if (error)
                        return reject(error);

                    resolve(res);
                })
            );
        }

        warn(message) {
            return this.emitEvent('log:warn', message);
        }

        startDemo(timeout) {
            return this.emitEvent('demo:broadcast:start', timeout);
        }

        startCollectTopology(timeout) {
            return this.emitEvent('start:collect:topology', timeout);
        }

        stopCollectTopology(demo) {
            return this.emitEvent('stop:collect:topology', demo);
        }

        /**
         * Execute REST request on node.
         *
         * @param {Command} cmd - REST command.
         * @return {Promise}
         */
        restCommand(cmd) {
            const params = {cmd: cmd._name};

            for (const param of cmd._params)
                params[param.key] = param.value;

            return this.emitEvent('node:rest', {uri: 'ignite', params, demo: cmd._demo, method: 'GET'})
                .then((res) => {
                    const code = res.code;

                    if (code === 401)
                        throw new Error('AgentSocket failed to authenticate in grid. Please check agent\'s login and password or node port.');

                    if (code !== 200)
                        throw new Error(res.error || 'Failed connect to node and execute REST command.');

                    const msg = JSON.parse(res.data);

                    if (msg.successStatus === 0)
                        return msg.response;

                    if (msg.successStatus === 2)
                        throw new Error('AgentSocket failed to authenticate in grid. Please check agent\'s login and password or node port.');

                    throw new Error(msg.error);
                });
        }

        gatewayCommand(demo, nids, taskCls, argCls, ...args) {
            const cmd = new Command(demo, 'exe')
                .addNamedParam('name', 'org.apache.ignite.internal.visor.compute.VisorGatewayTask')
                .addParam(nids)
                .addParam(taskCls)
                .addParam(argCls);

            _.forEach(args, (arg) => cmd.addParam(arg));

            return this.restCommand(cmd);
        }

        /**
         *
         * @param {Boolean} demo Is need run command on demo node.
         * @param {Boolean} attr Get attributes, if this parameter has value true. Default value: true.
         * @param {Boolean} mtr Get metrics, if this parameter has value true. Default value: false.
         * @returns {Promise}
         */
        topology(demo, attr, mtr) {
            const cmd = new Command(demo, 'top')
                .addNamedParam('attr', attr !== false)
                .addNamedParam('mtr', !!mtr);

            return this.restCommand(cmd);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} cacheName Cache name.
         * @returns {Promise}
         */
        metadata(demo, cacheName) {
            const cmd = new Command(demo, 'metadata')
                .addNamedParam('cacheName', cacheName);

            return this.restCommand(cmd);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} nid Node id.
         * @param {String} cacheName Cache name.
         * @param {String} query Query.
         * @param {Boolean} nonCollocatedJoins Flag whether to execute non collocated joins.
         * @param {Boolean} local Flag whether to execute query locally.
         * @param {int} pageSize Page size.
         * @returns {Promise}
         */
        fieldsQuery(demo, nid, cacheName, query, nonCollocatedJoins, local, pageSize) {
            if (nonCollocatedJoins) {
                return this.gatewayCommand(demo, nid,
                    'org.apache.ignite.internal.visor.query.VisorQueryTask',
                    'org.apache.ignite.internal.visor.query.VisorQueryArgV2',
                    cacheName, query, true, local, pageSize);
            }

            return this.gatewayCommand(demo, nid,
                'org.apache.ignite.internal.visor.query.VisorQueryTask',
                'org.apache.ignite.internal.visor.query.VisorQueryArg',
                cacheName, query, local, pageSize);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} nid Node id.
         * @param {int} queryId Query Id.
         * @param {int} pageSize Page size.
         * @returns {Promise}
         */
        queryFetch(demo, nid, queryId, pageSize) {
            return this.gatewayCommand(demo, nid,
                'org.apache.ignite.internal.visor.query.VisorQueryNextPageTask',
                'org.apache.ignite.lang.IgniteBiTuple', 'java.lang.String', 'java.lang.Integer',
                queryId, pageSize);
        }

        /**
         * @param {Boolean} demo Is need run command on demo node.
         * @param {String} nid Node id.
         * @param {int} queryId Query Id.
         * @returns {Promise}
         */
        queryClose(demo, nid, queryId) {
            return this.gatewayCommand(demo, '',
                'org.apache.ignite.internal.visor.query.VisorQueryCleanupTask',
                'java.util.Map', 'java.util.UUID', 'java.util.Set',
                `${nid}=${queryId}`);
        }
    }

    return AgentSocket;
};
