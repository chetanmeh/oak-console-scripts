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

import org.apache.jackrabbit.commons.JcrUtils
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

Repository repo = osgi.getService(Repository.class)
log = LoggerFactory.getLogger("script-console")

Session session = createAdminSession(repo)
String childNamePrefix = "child_with_a_very_long_name_so_that_doc_gets_filled_up_fast"
try{
    JNode node = JcrUtils.getOrCreateByPath("/content/test", "nt:unstructured", session)
    //Add a 15.8MB text property
    node.setProperty("bigProp", "x"*(1024*1024*15 + 1024*8))
    session.save()
    int count = 0
    while (true) {
        node.addNode(childNamePrefix + count++)
        if (count % 100 == 0) {
            try {
                node.setProperty("foo", "bar"*100 + count)
                session.save()
                print "Created $count nodes so far"
            } catch (Exception e ){
                print "Exception after $count nodes created"
                throw e
            }
        }
    }
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
