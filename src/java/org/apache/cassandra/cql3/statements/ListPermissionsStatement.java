/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;


import java.util.*;

import io.reactivex.Single;

import com.datastax.bdp.db.audit.AuditableEventType;
import com.datastax.bdp.db.audit.CoreAuditableEventType;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

public class ListPermissionsStatement extends AuthorizationStatement
{
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = "permissions"; // virtual cf to use for now.

    private static final List<ColumnSpecification> metadata = Collections.unmodifiableList(Arrays.asList(
        new ColumnSpecification(KS, CF, new ColumnIdentifier("role", true), UTF8Type.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("username", true), UTF8Type.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("resource", true), UTF8Type.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("permission", true), UTF8Type.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("granted", true), BooleanType.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("restricted", true), BooleanType.instance),
        new ColumnSpecification(KS, CF, new ColumnIdentifier("grantable", true), BooleanType.instance)
    ));

    protected final Set<Permission> permissions;
    protected IResource resource;
    protected final boolean recursive;
    private final RoleResource grantee;

    public ListPermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee, boolean recursive)
    {
        this.permissions = permissions;
        this.resource = resource;
        this.recursive = recursive;
        this.grantee = grantee.hasName() ? RoleResource.role(grantee.getName()) : null;
    }

    @Override
    public AuditableEventType getAuditEventType()
    {
        return CoreAuditableEventType.LIST_PERMISSIONS;
    }

    @Override
    public void validate(QueryState state)
    {
        if (!DatabaseDescriptor.getAuthorizer().requireAuthorization())
            throw invalidRequest(String.format("LIST PERMISSIONS operation is not supported by the %s if it is not enabled",
                                               DatabaseDescriptor.getAuthorizer().implementation().getClass().getSimpleName()));
    }

    @Override
    public void checkAccess(QueryState state)
    {
        // a check to ensure the existence of the user isn't being leaked by user existence check.
        state.checkNotAnonymous();

        if (resource != null)
        {
            resource = maybeCorrectResource(resource, state.getClientState());
            if (!resource.exists())
                throw invalidRequest("%s doesn't exist", resource);
        }

        if ((grantee != null) && !DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw invalidRequest("%s doesn't exist", grantee);

        // If the user requesting 'LIST PERMISSIONS' is not a superuser OR their username doesn't match 'grantee' OR
        // has no DESCRIBE permission on the role or all roles, we throw UnauthorizedException.
        // So only a superuser, system user can view everybody's permissions. Regular users are only
        // allowed to see their own permissions and those of roles for which they have DESCRIBE permission.
        if (!(state.isSuper() || state.isSystem())
            && !state.hasRole(grantee)
            && !state.hasRolePermission(grantee != null ? grantee : RoleResource.root(), CorePermission.DESCRIBE))
            throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                                                          grantee == null ? "everyone" : grantee.getRoleName()));
    }

    // TODO: Create a new ResultMessage type (?). Rows will do for now.
    public Single<ResultMessage> execute(QueryState state) throws RequestValidationException, RequestExecutionException
    {
        return Single.fromCallable(() -> {
            List<PermissionDetails> details = new ArrayList<>();

            if (resource != null && recursive)
            {
                for (IResource r : Resources.chain(resource))
                    details.addAll(list(r));
            }
            else
            {
                details.addAll(list(resource));
            }

            Collections.sort(details);
            return resultMessage(details);
        });
    }

    private Set<PermissionDetails> list(IResource resource)
    throws RequestValidationException, RequestExecutionException
    {
        try
        {
            return DatabaseDescriptor.getAuthorizer().list(permissions, resource, grantee);
        }
        catch (UnsupportedOperationException e)
        {
            throw new InvalidRequestException(e.getMessage());
        }
    }

    private ResultMessage resultMessage(List<PermissionDetails> details)
    {
        if (details.isEmpty())
            return new ResultMessage.Void();

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(metadata);
        ResultSet result = new ResultSet(resultMetadata);

        for (PermissionDetails pd : details)
        {
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.grantee));
            result.addColumnValue(UTF8Type.instance.decompose(pd.resource.toString()));
            result.addColumnValue(UTF8Type.instance.decompose(pd.permission.getFullName()));
            result.addColumnValue(BooleanType.instance.decompose(pd.modes.contains(GrantMode.GRANT)));
            result.addColumnValue(BooleanType.instance.decompose(pd.modes.contains(GrantMode.RESTRICT)));
            result.addColumnValue(BooleanType.instance.decompose(pd.modes.contains(GrantMode.GRANTABLE)));
        }
        return new ResultMessage.Rows(result);
    }
}
