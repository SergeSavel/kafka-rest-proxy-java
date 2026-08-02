// Copyright 2025 Sergey Savelev (serge@savel.pro)
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

package pro.savel.kafka.admin.responses;

import lombok.Getter;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class AdminDescribeUserScramCredentialsResponse extends
        ArrayList<AdminDescribeUserScramCredentialsResponse.ScramCredentialDescription> implements AdminResponse {

    private AdminDescribeUserScramCredentialsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    public static AdminDescribeUserScramCredentialsResponse of(Map<String, UserScramCredentialsDescription> source) {
        if (source == null)
            return null;
        var sourceDescriptions = source.values();
        var result = new AdminDescribeUserScramCredentialsResponse(sourceDescriptions.size());
        sourceDescriptions.forEach(sourceDescription -> result.add(ScramCredentialDescription.of(sourceDescription)));
        return result;
    }

    @Getter
    public static class ScramCredentialDescription {

        private String name;
        private Collection<ScramCredentialInfo> credentialInfos;

        private ScramCredentialDescription() {
        }

        public static ScramCredentialDescription of(UserScramCredentialsDescription source) {
            if (source == null)
                return null;
            var result = new ScramCredentialDescription();
            result.name = source.name();
            result.credentialInfos = ScramCredentialInfo.of(source.credentialInfos());
            return result;
        }
    }

    @Getter
    public static class ScramCredentialInfo {

        private String scramMechanism;
        private int iterations;

        private ScramCredentialInfo() {
        }

        public static ScramCredentialInfo of(org.apache.kafka.clients.admin.ScramCredentialInfo source) {
            if (source == null)
                return null;
            var result = new ScramCredentialInfo();
            result.scramMechanism = source.mechanism().mechanismName();
            result.iterations = source.iterations();
            return result;
        }

        private static ArrayList<ScramCredentialInfo> of(
                Collection<org.apache.kafka.clients.admin.ScramCredentialInfo> source) {
            if (source == null)
                return null;
            var result = new ArrayList<ScramCredentialInfo>(source.size());
            source.forEach(sourceItem -> result.add(of(sourceItem)));
            return result;
        }
    }
}
