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
import static org.junit.jupiter.api.Assertions.assertThrows;

class SaslConfigValidatorTest {

    private static final String SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule";

    @Test
    void rejectEmptyScramPassword_scramSha256WithEmptyPassword_throwsBadRequest() {
        var config = config("SCRAM-SHA-256", jaas("password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_scramSha512WithEmptyPassword_throwsBadRequest() {
        var config = config("SCRAM-SHA-512", jaas("password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_mechanismInLowerCaseWithEmptyPassword_throwsBadRequest() {
        var config = config("scram-sha-256", jaas("password=\"\""));
        assertThrows(BadRequestException.class, () -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_nonEmptyPassword_passes() {
        var config = config("SCRAM-SHA-256", jaas("password=\"secret\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_passwordOfEscapedQuote_passes() {
        var config = config("SCRAM-SHA-256", jaas("password=\"\\\"\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_plainMechanismWithEmptyPassword_passes() {
        var config = config("PLAIN",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"\";");
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_missingPasswordOption_passes() {
        var config = config("SCRAM-SHA-256", jaas(null));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_noJaasConfig_passes() {
        var config = config("SCRAM-SHA-256", null);
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_noMechanism_passes() {
        var config = config(null, jaas("password=\"\""));
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    @Test
    void rejectEmptyScramPassword_malformedJaasConfig_passes() {
        var config = config("SCRAM-SHA-256", "not a jaas config");
        assertDoesNotThrow(() -> SaslConfigValidator.rejectEmptyScramPassword(config));
    }

    private static Properties config(String mechanism, String jaasConfig) {
        var config = new Properties();
        if (mechanism != null)
            config.setProperty(SaslConfigs.SASL_MECHANISM, mechanism);
        if (jaasConfig != null)
            config.setProperty(SaslConfigs.SASL_JAAS_CONFIG, jaasConfig);
        return config;
    }

    private static String jaas(String passwordOption) {
        var builder = new StringBuilder(SCRAM_MODULE).append(" required username=\"u\" ");
        if (passwordOption != null)
            builder.append(passwordOption).append(' ');
        return builder.append(';').toString();
    }
}
