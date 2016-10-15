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

package org.apache.ignite.plugin.security;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteException;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

/**
 * Class for provide ability construct you permission set use
 * patter builder, you can create permission set inline.
 * Here is example:
 * <pre>
 *      SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()
 *              .appendCachePermissions("cache1", CACHE_PUT, CACHE_REMOVE)
 *              .appendCachePermissions("cache2", CACHE_READ)
 *              .appendTaskPermissions("task1", TASK_CANCEL)
 *              .appendTaskPermissions("task2", TASK_EXECUTE)
 *              .appendSystemPermissions(ADMIN_VIEW, EVENTS_ENABLE)
 *              .toSecurityPermissionSet();
 * </pre>
 *
 * if you will try append permission to inconsistent set, you get {@link IgniteException}.
 * Here is example:
 * <pre>
 *      SecurityPermissionSet permsSet = new SecurityPermissionSetBuilder()
 *          .appendCachePermissions("cache1", EVENTS_ENABLE, CACHE_REMOVE)
 *          .toSecurityPermissionSet();
 * </pre>
 *
 * will be {@link IgniteException} because you can't add EVENTS_ENABLE to cache permission.
 *
 */
public class SecurityPermissionSetBuilder {
    /** Cache permissions.*/
    private Map<String, Collection<SecurityPermission>> cachePerms = new HashMap<>();

    /** Task permissions.*/
    private Map<String, Collection<SecurityPermission>> taskPerms = new HashMap<>();

    /** System permissions.*/
    private List<SecurityPermission> sysPerms = new ArrayList<>();

    /** Default allow all.*/
    private boolean dfltAllowAll;

    /**
     * Static factory method for create new permission builder.
     *
     * @return SecurityPermissionSetBuilder
     */
    public static SecurityPermissionSetBuilder create(){
        return new SecurityPermissionSetBuilder();
    }
    /**
     * Append default all flag.
     *
     * @param dfltAllowAll Default allow all.
     * @return SecurityPermissionSetBuilder refer to same permission builder.
     */
    public SecurityPermissionSetBuilder defaultAllowAll(boolean dfltAllowAll) {
        this.dfltAllowAll = dfltAllowAll;

        return this;
    }

    /**
     * Append permission set form {@link org.apache.ignite.IgniteCompute task} with {@code name}.
     *
     * @param name  String for map some task to permission set.
     * @param perms Permissions.
     * @return SecurityPermissionSetBuilder refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendTaskPermissions(String name, SecurityPermission... perms) {
        validate(toCollection("TASK_"), perms);

        append(taskPerms, name, toCollection(perms));

        return this;
    }

    /**
     * Append permission set form {@link org.apache.ignite.IgniteCache cache} with {@code name}.
     *
     * @param name  String for map some cache to permission set.
     * @param perms Permissions.
     * @return {@link SecurityPermissionSetBuilder} refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendCachePermissions(String name, SecurityPermission... perms) {
        validate(toCollection("CACHE_"), perms);

        append(cachePerms, name, toCollection(perms));

        return this;
    }

    /**
     * Append system permission set.
     *
     * @param perms Permission.
     * @return {@link SecurityPermissionSetBuilder} refer to same permission builder.
     */
    public SecurityPermissionSetBuilder appendSystemPermissions(SecurityPermission... perms) {
        validate(toCollection("EVENTS_", "ADMIN_"), perms);

        sysPerms.addAll(toCollection(perms));

        return this;
    }

    /**
     * Validate method use patterns.
     *
     * @param ptrns Pattern.
     * @param perms Permissions.
     */
    private void validate(Collection<String> ptrns, SecurityPermission... perms) {
        assert ptrns != null;
        assert perms != null;

        for (SecurityPermission perm : perms)
            validate(ptrns, perm);
    }

    /**
     * @param ptrns Patterns.
     * @param perm  Permission.
     */
    private void validate(Collection<String> ptrns, SecurityPermission perm) {
        assert ptrns != null;
        assert perm != null;

        boolean ex = true;

        String name = perm.name();

        for (String ptrn : ptrns) {
            if (name.startsWith(ptrn)) {
                ex = false;
                break;
            }
        }

        if (ex)
            throw new IgniteException(
                    "you can assign permission only start with " + ptrns + ", but you try " + name
            );
    }

    /**
     * Convert vararg to {@link Collection}.
     *
     * @param perms Permissions.
     */
    @SafeVarargs
    private final <T> Collection<T> toCollection(T... perms) {
        assert perms != null;

        List<T> col = new ArrayList<>();

        Collections.addAll(col, perms);

        return col;
    }

    /**
     * @param permsMap Permissions map.
     * @param name     Name.
     * @param perms    Permission.
     */
    private void append(
            Map<String, Collection<SecurityPermission>> permsMap,
            String name,
            Collection<SecurityPermission> perms
    ) {
        assert permsMap != null;
        assert name != null;
        assert perms != null;

        Collection<SecurityPermission> col = permsMap.get(name);

        if (col == null)
            permsMap.put(name, perms);
        else
            col.addAll(perms);
    }

    /**
     * Convert internal state of {@link SecurityPermissionSetBuilder} to {@link SecurityPermissionSet}.
     *
     * @return {@link SecurityPermissionSet}
     */
    public SecurityPermissionSet toSecurityPermissionSet() {
        SecurityBasicPermissionSet permSet = new SecurityBasicPermissionSet();

        permSet.setDefaultAllowAll(dfltAllowAll);
        permSet.setCachePermissions(unmodifiableMap(cachePerms));
        permSet.setTaskPermissions(unmodifiableMap(taskPerms));
        permSet.setSysPermissions(unmodifiableList(sysPerms));

        return permSet;
    }
}
