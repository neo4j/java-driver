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
package org.neo4j.driver.internal.bolt.api.ssl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * A tool used to certs.
 */
final class CertificateTool {
    /**
     * Load the certificates written in X.509 format in a file to a key store.
     *
     * @param certFiles the certificate files
     * @param keyStore the key store
     */
    public static void loadX509Cert(List<File> certFiles, KeyStore keyStore)
            throws GeneralSecurityException, IOException {
        var certCount = 0; // The files might contain multiple certs
        for (var certFile : certFiles) {
            try (var inputStream = new BufferedInputStream(new FileInputStream(certFile))) {
                var certFactory = CertificateFactory.getInstance("X.509");

                while (inputStream.available() > 0) {
                    try {
                        var cert = certFactory.generateCertificate(inputStream);
                        certCount++;
                        loadX509Cert(cert, "neo4j.javadriver.trustedcert." + certCount, keyStore);
                    } catch (CertificateException e) {
                        if (e.getCause() != null && e.getCause().getMessage().equals("Empty input")) {
                            // This happens if there is whitespace at the end of the certificate - we load one cert, and
                            // then try and load a
                            // second cert, at which point we fail
                            return;
                        }
                        throw new IOException(
                                "Failed to load certificate from `" + certFile.getAbsolutePath() + "`: " + certCount
                                        + " : " + e.getMessage(),
                                e);
                    }
                }
            }
        }
    }

    public static void loadX509Cert(X509Certificate[] certificates, KeyStore keyStore) throws GeneralSecurityException {
        for (var i = 0; i < certificates.length; i++) {
            loadX509Cert(certificates[i], "neo4j.javadriver.trustedcert." + i, keyStore);
        }
    }

    private static void loadX509Cert(Certificate cert, String certAlias, KeyStore keyStore) throws KeyStoreException {
        keyStore.setCertificateEntry(certAlias, cert);
    }

    private CertificateTool() {}
}
