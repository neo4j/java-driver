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
package org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.driver.internal.bolt.api.BoltServerAddress.LOCAL_DEFAULT;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.creationTimestamp;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.messageDispatcher;
import static org.neo4j.driver.internal.bolt.basicimpl.impl.async.connection.ChannelAttributes.serverAddress;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Clock;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.internal.bolt.api.BoltServerAddress;
import org.neo4j.driver.internal.bolt.api.SecurityPlan;
import org.neo4j.driver.internal.bolt.basicimpl.impl.NoopLoggingProvider;

class NettyChannelInitializerTest {
    private final EmbeddedChannel channel = new EmbeddedChannel();

    @AfterEach
    void tearDown() {
        channel.finishAndReleaseAll();
    }

    @Test
    void shouldAddSslHandlerWhenRequiresEncryption() {
        var security = trustAllCertificates(false);
        var initializer = newInitializer(security);

        initializer.initChannel(channel);

        assertNotNull(channel.pipeline().get(SslHandler.class));
    }

    @Test
    void shouldNotAddSslHandlerWhenDoesNotRequireEncryption() {
        var security = SecurityPlan.INSECURE;
        var initializer = newInitializer(security);

        initializer.initChannel(channel);

        assertNull(channel.pipeline().get(SslHandler.class));
    }

    @Test
    void shouldAddSslHandlerWithHandshakeTimeout() {
        var timeoutMillis = 424242;
        var security = trustAllCertificates(false);
        var initializer = newInitializer(security, timeoutMillis);

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        assertNotNull(sslHandler);
        assertEquals(timeoutMillis, sslHandler.getHandshakeTimeoutMillis());
    }

    @Test
    void shouldUpdateChannelAttributes() {
        var clock = mock(Clock.class);
        when(clock.millis()).thenReturn(42L);
        var security = SecurityPlan.INSECURE;
        var initializer = newInitializer(security, Integer.MAX_VALUE, clock);

        initializer.initChannel(channel);

        assertEquals(LOCAL_DEFAULT, serverAddress(channel));
        assertEquals(42L, creationTimestamp(channel));
        assertNotNull(messageDispatcher(channel));
    }

    @Test
    void shouldIncludeSniHostName() {
        var address = new BoltServerAddress("database.neo4j.com", 8989);
        var initializer = new NettyChannelInitializer(
                address, trustAllCertificates(false), 10000, Clock.systemUTC(), NoopLoggingProvider.INSTANCE);

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        var sslEngine = sslHandler.engine();
        var sslParameters = sslEngine.getSSLParameters();
        var sniServerNames = sslParameters.getServerNames();
        assertEquals(1, sniServerNames.size());
        assertInstanceOf(SNIHostName.class, sniServerNames.get(0));
        assertEquals(address.host(), ((SNIHostName) sniServerNames.get(0)).getAsciiName());
    }

    @Test
    void shouldEnableHostnameVerificationWhenConfigured() {
        testHostnameVerificationSetting(true, "HTTPS");
    }

    @Test
    void shouldNotEnableHostnameVerificationWhenNotConfigured() {
        testHostnameVerificationSetting(false, null);
    }

    private void testHostnameVerificationSetting(boolean enabled, String expectedValue) {
        var initializer = newInitializer(trustAllCertificates(enabled));

        initializer.initChannel(channel);

        var sslHandler = channel.pipeline().get(SslHandler.class);
        var sslEngine = sslHandler.engine();
        var sslParameters = sslEngine.getSSLParameters();
        assertEquals(expectedValue, sslParameters.getEndpointIdentificationAlgorithm());
    }

    private static NettyChannelInitializer newInitializer(SecurityPlan securityPlan) {
        return newInitializer(securityPlan, Integer.MAX_VALUE);
    }

    private static NettyChannelInitializer newInitializer(SecurityPlan securityPlan, int connectTimeoutMillis) {
        return newInitializer(securityPlan, connectTimeoutMillis, Clock.systemUTC());
    }

    private static NettyChannelInitializer newInitializer(
            SecurityPlan securityPlan, int connectTimeoutMillis, Clock clock) {
        return new NettyChannelInitializer(
                LOCAL_DEFAULT, securityPlan, connectTimeoutMillis, clock, NoopLoggingProvider.INSTANCE);
    }

    private static SecurityPlan trustAllCertificates(boolean enabled) {
        return new SecurityPlan() {
            @Override
            public boolean requiresEncryption() {
                return true;
            }

            @Override
            public boolean requiresClientAuth() {
                return false;
            }

            @Override
            public SSLContext sslContext() {
                SSLContext sslContext;
                try {
                    sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(new KeyManager[0], new TrustManager[] {new TrustAllTrustManager()}, null);
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    throw new RuntimeException(e);
                }
                return sslContext;
            }

            @Override
            public boolean requiresHostnameVerification() {
                return enabled;
            }

            private static class TrustAllTrustManager implements X509TrustManager {
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    throw new CertificateException("All client connections to this client are forbidden.");
                }

                public void checkServerTrusted(X509Certificate[] chain, String authType) {
                    // all fine, pass through
                }

                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }
            }
        };
    }
}
