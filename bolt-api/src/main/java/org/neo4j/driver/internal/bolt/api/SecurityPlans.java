/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.driver.internal.bolt.api;

import java.io.IOException;
import java.security.GeneralSecurityException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import org.neo4j.driver.internal.bolt.api.ssl.RevocationCheckingStrategy;
import org.neo4j.driver.internal.bolt.api.ssl.SSLContexts;
import org.neo4j.driver.internal.bolt.api.ssl.TrustManagerFactories;

public final class SecurityPlans {
    private static final SecurityPlan UNENCRYPTED = new SecurityPlanImpl(false, false, null, false);

    public static SecurityPlan encrypted(
            boolean requiresClientAuth, SSLContext sslContext, boolean requiresHostnameVerification) {
        return new SecurityPlanImpl(true, requiresClientAuth, sslContext, requiresHostnameVerification);
    }

    public static SecurityPlan encryptedForAnyCertificate() throws GeneralSecurityException {
        var sslContext = SSLContexts.forAnyCertificate(new KeyManager[0]);
        return encrypted(false, sslContext, false);
    }

    public static SecurityPlan encryptedForSystemCASignedCertificates() throws GeneralSecurityException, IOException {
        var trustManagerFactory = TrustManagerFactories.forSystemCertificates(RevocationCheckingStrategy.NO_CHECKS);
        var sslContext = SSLContexts.forTrustManagers(new KeyManager[0], trustManagerFactory.getTrustManagers());
        return encrypted(false, sslContext, true);
    }

    public static SecurityPlan unencrypted() {
        return UNENCRYPTED;
    }

    private SecurityPlans() {}
}
