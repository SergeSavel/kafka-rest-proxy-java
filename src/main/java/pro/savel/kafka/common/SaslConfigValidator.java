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
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import pro.savel.kafka.common.exceptions.BadRequestException;

import java.util.Map;
import java.util.Properties;

public abstract class SaslConfigValidator {

    private static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
    private static final String SCRAM_SHA_512 = "SCRAM-SHA-512";
    private static final String PASSWORD_OPTION = "password";

    public static void rejectEmptyScramPassword(Properties config) {
        var mechanism = config.getProperty(SaslConfigs.SASL_MECHANISM);
        if (!SCRAM_SHA_256.equalsIgnoreCase(mechanism) && !SCRAM_SHA_512.equalsIgnoreCase(mechanism))
            return;
        var jaasConfig = config.getProperty(SaslConfigs.SASL_JAAS_CONFIG);
        if (jaasConfig == null)
            return;
        String password;
        try {
            var context = JaasContext.loadClientContext(Map.of(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfig)));
            password = JaasContext.configEntryOption(context.configurationEntries(), PASSWORD_OPTION,
                    ScramLoginModule.class.getName());
        } catch (RuntimeException ignored) {
            return;
        }
        if (password != null && password.isEmpty())
            throw new BadRequestException("Empty SCRAM password in sasl.jaas.config.");
    }
}
