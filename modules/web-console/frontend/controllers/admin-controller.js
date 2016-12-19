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

// Controller for Admin screen.
export default ['adminController', [
    '$rootScope', '$scope', '$http', '$q', '$state', 'IgniteMessages', 'IgniteConfirm', 'User', 'IgniteNotebookData', 'IgniteCountries',
    ($rootScope, $scope, $http, $q, $state, Messages, Confirm, User, Notebook, Countries) => {
        $scope.users = null;

        const _reloadUsers = () => {
            $http.post('/api/v1/admin/list')
                .then(({data}) => {
                    $scope.users = data;

                    _.forEach($scope.users, (user) => {
                        user.userName = user.firstName + ' ' + user.lastName;
                        user.countryCode = Countries.getByName(user.country).code;
                        user.label = user.userName + ' ' + user.email + ' ' +
                            (user.company || '') + ' ' + (user.countryCode || '');
                    });
                })
                .catch(Messages.showError);
        };

        _reloadUsers();

        $scope.becomeUser = function(user) {
            $http.get('/api/v1/admin/become', { params: {viewedUserId: user._id}})
                .then(() => User.load())
                .then((becomeUser) => {
                    $rootScope.$broadcast('user', becomeUser);

                    $state.go('base.configuration.clusters');
                })
                .then(() => Notebook.load())
                .catch(Messages.showError);
        };

        $scope.removeUser = (user) => {
            Confirm.confirm(`Are you sure you want to remove user: "${user.userName}"?`)
                .then(() => {
                    $http.post('/api/v1/admin/remove', {userId: user._id})
                        .then(() => {
                            const i = _.findIndex($scope.users, (u) => u._id === user._id);

                            if (i >= 0)
                                $scope.users.splice(i, 1);

                            Messages.showInfo(`User has been removed: "${user.userName}"`);
                        })
                        .catch(({data, status}) => {
                            if (status === 503)
                                Messages.showInfo(data);
                            else
                                Messages.showError('Failed to remove user: ', data);
                        });
                });
        };

        $scope.toggleAdmin = (user) => {
            if (user.adminChanging)
                return;

            user.adminChanging = true;

            $http.post('/api/v1/admin/save', {userId: user._id, adminFlag: !user.admin})
                .then(() => {
                    user.admin = !user.admin;

                    Messages.showInfo(`Admin right was successfully toggled for user: "${user.userName}"`);
                })
                .catch((res) => {
                    Messages.showError('Failed to toggle admin right for user: ', res);
                })
                .finally(() => user.adminChanging = false);
        };
    }
]];
