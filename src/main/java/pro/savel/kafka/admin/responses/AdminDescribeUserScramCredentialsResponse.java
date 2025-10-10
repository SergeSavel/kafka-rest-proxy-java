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

import lombok.Data;

import java.util.ArrayList;

public class AdminDescribeUserScramCredentialsResponse extends ArrayList<AdminDescribeUserScramCredentialsResponse.ScramCredentialDescription> implements AdminResponse {

    public AdminDescribeUserScramCredentialsResponse(int initialCapacity) {
        super(initialCapacity);
    }

    @Data
    public static class ScramCredentialDescription {
        String name;
        ArrayList<ScramCredentialInfo> credentialInfos;
    }

    @Data
    public static class ScramCredentialInfo {
        String scramMechanism;
        int iterations;
    }
}
