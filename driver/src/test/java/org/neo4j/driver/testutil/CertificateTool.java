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
package org.neo4j.driver.testutil;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Base64;
import java.util.List;

/**
 * A tool used to save, load certs, etc.
 */
final class CertificateTool {
    private static final String BEGIN_CERT = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERT = "-----END CERTIFICATE-----";

    /**
     * Save a certificate to a file. Remove all the content in the file if there is any before.
     *
     * @param cert the certificate
     * @param certFile the certificate file
     */
    public static void saveX509Cert(Certificate cert, File certFile) throws GeneralSecurityException, IOException {
        saveX509Cert(new Certificate[] {cert}, certFile);
    }

    /**
     * Save a list of certificates into a file
     *
     * @param certs the certificates
     * @param certFile the certificate file
     */
    public static void saveX509Cert(Certificate[] certs, File certFile) throws GeneralSecurityException, IOException {
        try (var writer = new BufferedWriter(new FileWriter(certFile))) {
            for (var cert : certs) {
                var certStr =
                        Base64.getEncoder().encodeToString(cert.getEncoded()).replaceAll("(.{64})", "$1\n");

                writer.write(BEGIN_CERT);
                writer.newLine();

                writer.write(certStr);
                writer.newLine();

                writer.write(END_CERT);
                writer.newLine();
            }
        }
    }

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

    /**
     * Load a certificate to a key store with a name
     *
     * @param certAlias a name to identify different certificates
     * @param cert the certificate
     * @param keyStore the key store
     */
    public static void loadX509Cert(Certificate cert, String certAlias, KeyStore keyStore) throws KeyStoreException {
        keyStore.setCertificateEntry(certAlias, cert);
    }

    private CertificateTool() {}
}
