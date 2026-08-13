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

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

class HttpUtilsTest {

    @Test
    void mediaType_stripsParametersTrimsAndLowercases() {
        assertEquals("application/json", HttpUtils.mediaType("application/json"));
        assertEquals("application/json", HttpUtils.mediaType("application/json; charset=utf-8"));
        assertEquals("application/json", HttpUtils.mediaType("application/json;charset=utf-8"));
        assertEquals("application/json", HttpUtils.mediaType("  Application/JSON ; charset=UTF-8"));
        assertEquals("application/octet-stream", HttpUtils.mediaType("application/octet-stream; q=1"));
    }

    @Test
    void mediaType_nullOrBlank_returnsNull() {
        assertNull(HttpUtils.mediaType(null));
        assertNull(HttpUtils.mediaType(""));
        assertNull(HttpUtils.mediaType("   "));
        assertNull(HttpUtils.mediaType("; charset=utf-8"));
    }

    @Test
    void isJson_recognizesMediaTypeRegardlessOfParametersAndCase() {
        assertTrue(HttpUtils.isJson("application/json"));
        assertTrue(HttpUtils.isJson("application/json; charset=utf-8"));
        assertTrue(HttpUtils.isJson("application/json;charset=utf-8"));
        assertTrue(HttpUtils.isJson("Application/JSON"));
        assertFalse(HttpUtils.isJson("text/plain"));
        assertFalse(HttpUtils.isJson(null));
    }

    @Test
    void mediaType_isLocaleIndependent() {
        // Turkish lowercases 'I' to dotless 'ı', which would break "application" for a default-locale
        // toLowerCase(). Header values are protocol data, so they must fold the same everywhere.
        var previousDefault = Locale.getDefault();
        Locale.setDefault(Locale.forLanguageTag("tr-TR"));
        try {
            assertEquals("application/json", HttpUtils.mediaType("APPLICATION/JSON"));
            assertTrue(HttpUtils.isJson("Application/JSON; charset=UTF-8"));
            assertTrue(HttpUtils.isOctetStream("APPLICATION/OCTET-STREAM"));
        } finally {
            Locale.setDefault(previousDefault);
        }
    }

    @Test
    void isOctetStream_recognizesMediaTypeRegardlessOfParametersAndCase() {
        assertTrue(HttpUtils.isOctetStream("application/octet-stream"));
        assertTrue(HttpUtils.isOctetStream("application/octet-stream; boundary=x"));
        assertTrue(HttpUtils.isOctetStream("Application/Octet-Stream"));
        assertFalse(HttpUtils.isOctetStream("application/json"));
        assertFalse(HttpUtils.isOctetStream(null));
    }
}
