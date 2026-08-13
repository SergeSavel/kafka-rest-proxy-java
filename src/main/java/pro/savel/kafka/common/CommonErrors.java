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

import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.errors.*;
import pro.savel.kafka.common.exceptions.HttpStatusException;

public abstract class CommonErrors {
    public static boolean handle(ChannelHandlerContext ctx, Throwable error) {
        var handled = true;
        switch (error) {
            case HttpStatusException ge ->
                    HttpUtils.writeHttpResponseAndClose(ctx, ge.status(), Utils.combineErrorMessage(error));
            // No Thread.currentThread().interrupt() here: handle() runs on whatever thread delivered
            // the error (a Netty event loop, a Kafka producer/admin I/O thread), not on the thread
            // that was actually interrupted, so setting the flag would only corrupt that thread.
            case InterruptedException ignored ->
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            // Kafka's wrapper over InterruptedException, thrown out of the client APIs. Nothing to
            // restore here either: its constructor already re-set the flag, and it did so on the
            // thread that was really interrupted rather than on this one.
            case InterruptException ignored ->
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            case IllegalArgumentException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case IllegalStateException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case WakeupException ignored ->
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            case TimeoutException ignored ->
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            case AuthorizationException ignored ->
                    HttpUtils.writeForbiddenAndClose(ctx, Utils.combineErrorMessage(error));
            case AuthenticationException ignored ->
                    HttpUtils.writeUnauthorizedAndClose(ctx, Utils.combineErrorMessage(error));
            case InvalidRequestException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case InvalidOffsetException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case ResourceNotFoundException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case DuplicateResourceException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case NotControllerException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case UnsupportedByAuthenticationException ignored ->
                    HttpUtils.writeUnauthorizedAndClose(ctx, Utils.combineErrorMessage(error));
            case UnacceptableCredentialException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case UnknownTopicOrPartitionException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case UnknownTopicIdException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case InvalidConfigurationException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case GroupNotEmptyException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case GroupSubscribedToTopicException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case UnknownMemberIdException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case TopicExistsException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case ReassignmentInProgressException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case BrokerNotAvailableException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case FencedInstanceIdException ignored ->
                    HttpUtils.writeConflictAndClose(ctx, Utils.combineErrorMessage(error));
            case FeatureUpdateFailedException ignored ->
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            case null, default -> handled = false;
        }
        return handled;
    }
}
