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
import pro.savel.kafka.common.exceptions.*;

public abstract class CommonErrors {
    public static boolean handle(ChannelHandlerContext ctx, Throwable error) {
        var handled = true;
        switch (error) {
            case BadRequestException ignored ->
                    HttpUtils.writeBadRequestAndClose(ctx, Utils.combineErrorMessage(error));
            case NotFoundException ignored ->
                    HttpUtils.writeNotFoundAndClose(ctx, Utils.combineErrorMessage(error));
            case MethodNotAllowedException ignored ->
                    HttpUtils.writeMethodNotAllowedAndClose(ctx, Utils.combineErrorMessage(error));
            case UnauthenticatedException ignored ->
                    HttpUtils.writeUnauthorizedAndClose(ctx, Utils.combineErrorMessage(error));
            case UnauthorizedException ignored ->
                    HttpUtils.writeForbiddenAndClose(ctx, Utils.combineErrorMessage(error));
            case InterruptedException ignored -> {
                    Thread.currentThread().interrupt();
                    HttpUtils.writeInternalServerErrorAndClose(ctx, Utils.combineErrorMessage(error));
            }
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
            case null, default -> handled = false;
        }
        return handled;
    }
}
