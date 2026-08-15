// Copyright 2026 Sergey Savelev (serge@savel.pro)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.kafka.common;

import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SaslConfigValidatorTest {

    private static final String SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";

    @Test
    void rejectEmptyScramCredentials_scramSha256WithEmptyPassword_throwsBadRequest() {
        var config = config("SCRAM-SHA-256", jaas("username=\"u\"", "password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_scramSha512WithEmptyPassword_throwsBadRequest() {
        var config = config("SCRAM-SHA-512", jaas("username=\"u\"", "password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_mechanismInLowerCaseWithEmptyPassword_throwsBadRequest() {
        var config = config("scram-sha-256", jaas("username=\"u\"", "password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_emptyUsername_throwsBadRequest() {
        var config = config("SCRAM-SHA-256", jaas("username=\"\"", "password=\"secret\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_emptyUsernameAndEmptyPassword_throwsBadRequest() {
        var config = config("SCRAM-SHA-256", jaas("username=\"\"", "password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_nonEmptyCredentials_passes() {
        var config = config("SCRAM-SHA-256", jaas("username=\"u\"", "password=\"secret\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_passwordOfEscapedQuote_passes() {
        var config = config("SCRAM-SHA-256", jaas("username=\"u\"", "password=\"\\\"\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_plainMechanismWithEmptyCredentials_passes() {
        var config = config("PLAIN",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"\" password=\"\";");
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_missingUsernameOption_passes() {
        var config = config("SCRAM-SHA-256", jaas(null, "password=\"secret\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_missingPasswordOption_passes() {
        var config = config("SCRAM-SHA-256", jaas("username=\"u\"", null));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_noJaasConfig_passes() {
        var config = config("SCRAM-SHA-256", null);
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_noMechanism_passes() {
        var config = config(null, jaas("username=\"\"", "password=\"\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void rejectEmptyScramCredentials_malformedJaasConfig_passes() {
        var config = config("SCRAM-SHA-256", "not a jaas config");
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramCredentials(config));
    }

    @Test
    void usernameFromJaasConfig_scramModule_returnsUsername() {
        var config = config("SCRAM-SHA-256", jaas("username=\"scram-user\"", "password=\"secret\""));
        assertEquals("scram-user", SaslConfigValidator.usernameFromJaasConfig(config));
    }

    @Test
    void usernameFromJaasConfig_plainModule_returnsUsername() {
        var config = config("PLAIN",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"plain-user\" password=\"secret\";");
        assertEquals("plain-user", SaslConfigValidator.usernameFromJaasConfig(config));
    }

    @Test
    void usernameFromJaasConfig_noUsernameOption_returnsNull() {
        var config = config("SCRAM-SHA-256", jaas(null, "password=\"secret\""));
        assertNull(SaslConfigValidator.usernameFromJaasConfig(config));
    }

    @Test
    void usernameFromJaasConfig_noJaasConfig_returnsNull() {
        var config = config("SCRAM-SHA-256", null);
        assertNull(SaslConfigValidator.usernameFromJaasConfig(config));
    }

    @Test
    void usernameFromJaasConfig_malformedJaasConfig_returnsNull() {
        var config = config("SCRAM-SHA-256", "not a jaas config");
        assertNull(SaslConfigValidator.usernameFromJaasConfig(config));
    }

    private static Properties config(String mechanism, String jaasConfig) {
        var config = new Properties();
        if (mechanism != null)
            config.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
        if (jaasConfig != null)
            config.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        return config;
    }

    private static String jaas(String usernameOption, String passwordOption) {
        var builder = new StringBuilder(SCRAM_MODULE).append(" required ");
        if (usernameOption != null)
            builder.append(usernameOption).append(' ');
        if (passwordOption != null)
            builder.append(passwordOption).append(' ');
        return builder.append(';').toString();
    }
}
