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

import jakarta.validation.ConstraintViolation;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class UtilsTest {

//region combineErrorMessage

    @Test
    void combineErrorMessage_nullThrowable_returnsNull() {
        assertNull(Utils.combineErrorMessage(null));
    }

    @Test
    void combineErrorMessage_singleMessage_returnsMessage() {
        var ex = new RuntimeException("error");
        assertEquals("error", Utils.combineErrorMessage(ex));
    }

    @Test
    void combineErrorMessage_chainedCauses_returnsAllMessages() {
        var root = new RuntimeException("root cause");
        var mid = new RuntimeException("mid", root);
        var top = new RuntimeException("top", mid);
        assertEquals("top\nmid\nroot cause", Utils.combineErrorMessage(top));
    }

    @Test
    void combineErrorMessage_nullMessageInChain_skipsNull() {
        var root = new RuntimeException("root");
        var mid = new RuntimeException((String) null, root);
        var top = new RuntimeException("top", mid);
        assertEquals("top\nroot", Utils.combineErrorMessage(top));
    }

    @Test
    void combineErrorMessage_allNullMessages_returnsNull() {
        var ex = new RuntimeException((String) null, new RuntimeException((String) null));
        assertNull(Utils.combineErrorMessage(ex));
    }

    @Test
    void combineErrorMessage_exceptionWithNoMessage_returnsNull() {
        var ex = new NullPointerException();
        assertNull(Utils.combineErrorMessage(ex));
    }

//endregion

//region combineConstraintViolationMessage (single)

    @Test
    void combineConstraintViolationMessage_singleNull_returnsNull() {
        assertNull(Utils.combineConstraintViolationMessage((ConstraintViolation<Object>) null));
    }

    @Test
    void combineConstraintViolationMessage_single_returnsFormatted() {
        var violation = mock(ConstraintViolation.class);
        when(violation.getPropertyPath()).thenReturn(mock(jakarta.validation.Path.class));
        when(violation.getPropertyPath().toString()).thenReturn("name");
        when(violation.getMessage()).thenReturn("must not be empty");
        assertEquals("name: must not be empty", Utils.combineConstraintViolationMessage(violation));
    }

//endregion

//region combineConstraintViolationMessage (collection)

    @Test
    void combineConstraintViolationMessage_collectionNull_returnsNull() {
        assertNull(Utils.combineConstraintViolationMessage((Set<ConstraintViolation<Object>>) null));
    }

    @Test
    void combineConstraintViolationMessage_collectionEmpty_returnsEmpty() {
        assertEquals("", Utils.combineConstraintViolationMessage(List.of()));
    }

    @Test
    void combineConstraintViolationMessage_collectionMultiple_returnsJoined() {
        var v1 = mock(ConstraintViolation.class);
        when(v1.getPropertyPath()).thenReturn(mock(jakarta.validation.Path.class));
        when(v1.getPropertyPath().toString()).thenReturn("name");
        when(v1.getMessage()).thenReturn("must not be empty");

        var v2 = mock(ConstraintViolation.class);
        when(v2.getPropertyPath()).thenReturn(mock(jakarta.validation.Path.class));
        when(v2.getPropertyPath().toString()).thenReturn("age");
        when(v2.getMessage()).thenReturn("must be positive");

        var result = Utils.combineConstraintViolationMessage(List.of(v1, v2));
        assertEquals("name: must not be empty\nage: must be positive", result);
    }

//endregion
}
