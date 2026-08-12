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

package pro.savel.kafka.admin.responses;

import lombok.Getter;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.SupportedVersionRange;

import java.util.ArrayList;

@Getter
public class AdminDescribeFeaturesResponse implements AdminResponse {

    private ArrayList<SupportedFeature> supportedFeatures;
    private ArrayList<FinalizedFeature> finalizedFeatures;
    private Long finalizedFeaturesEpoch;

    private AdminDescribeFeaturesResponse() {
    }

    public static AdminDescribeFeaturesResponse of(FeatureMetadata source) {
        if (source == null)
            return null;
        var result = new AdminDescribeFeaturesResponse();
        result.supportedFeatures = new ArrayList<>(source.supportedFeatures().size());
        source.supportedFeatures().forEach((name, range) -> result.supportedFeatures.add(SupportedFeature.of(name, range)));
        result.finalizedFeatures = new ArrayList<>(source.finalizedFeatures().size());
        source.finalizedFeatures().forEach((name, range) -> result.finalizedFeatures.add(FinalizedFeature.of(name, range)));
        result.finalizedFeaturesEpoch = source.finalizedFeaturesEpoch().orElse(null);
        return result;
    }

    @Getter
    public static class SupportedFeature {

        private String name;
        private short minVersion;
        private short maxVersion;

        private static SupportedFeature of(String name, SupportedVersionRange source) {
            var result = new SupportedFeature();
            result.name = name;
            if (source != null) {
                result.minVersion = source.minVersion();
                result.maxVersion = source.maxVersion();
            }
            return result;
        }
    }

    @Getter
    public static class FinalizedFeature {

        private String name;
        private short minVersionLevel;
        private short maxVersionLevel;

        private static FinalizedFeature of(String name, FinalizedVersionRange source) {
            var result = new FinalizedFeature();
            result.name = name;
            if (source != null) {
                result.minVersionLevel = source.minVersionLevel();
                result.maxVersionLevel = source.maxVersionLevel();
            }
            return result;
        }
    }
}
