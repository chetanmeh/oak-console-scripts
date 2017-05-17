/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package felixconsole.changent

import org.apache.jackrabbit.oak.spi.security.authentication.PreAuthenticatedLogin
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal
import org.slf4j.LoggerFactory

import javax.jcr.Node as JNode
import javax.jcr.Repository
import javax.jcr.Session
import javax.security.auth.Subject
import java.security.Principal
import java.security.PrivilegedExceptionAction

import static java.util.Collections.emptySet

/**
 * Script to change the nodetype of any path to a new nodetype.
 * This operation is not supported by any UI gesture hence the
 * need for this script
 */

log = LoggerFactory.getLogger("script-console")
Repository repo = osgi.getService(Repository.class)

/**
 * Specify the path here whose nodetype needs to be changed
 */
String path = "/content/test"

/**
 * Name of new nodetype
 */
String newNodeType = "oak:Unstructured"

Session session = createAdminSession(repo)
try{
    JNode node = session.getNode(path)
    print "Existing node type ${node.getPrimaryNodeType().getName()}"
    node.setPrimaryType(newNodeType)
    session.save()
    print "New nodetype. ${session.getNode(path).getPrimaryNodeType().getName()}"
} finally {
    session?.logout()
}


Session createAdminSession(Repository repository) {
    Set<? extends Principal> principals = [{'admin'} as AdminPrincipal]
    Subject subject = new Subject(true, principals, [PreAuthenticatedLogin.PRE_AUTHENTICATED] as HashSet, emptySet())
    return Subject.doAsPrivileged(subject,
            {repository.login(null, null)} as PrivilegedExceptionAction<Session>,
            null)
}

def print(String msg){
    println msg
    log.info(msg)
}
