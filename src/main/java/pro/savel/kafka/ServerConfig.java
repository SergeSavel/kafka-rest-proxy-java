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

package pro.savel.kafka;

record ServerConfig(
        String host,
        int port,
        int workerThreads,
        int backlog,
        int readTimeoutSeconds,
        int writeTimeoutSeconds,
        int maxRequestBytes,
        int maxJsonRequestBytes,
        int responseChunkBytes,
        boolean epollEnabled) {

    private static final int DEFAULT_MAX_REQUEST_BYTES = 32 * 1024 * 1024;
    private static final int DEFAULT_MAX_JSON_REQUEST_BYTES = 4 * 1024 * 1024;

    ServerConfig {
        if (host == null || host.isBlank())
            throw new IllegalArgumentException("host must not be blank");
        if (port < 1 || port > 65_535)
            throw new IllegalArgumentException("port must be between 1 and 65535");
        if (workerThreads < 0)
            throw new IllegalArgumentException("netty.workerThreads must be greater than or equal to 0");
        requirePositive("netty.backlog", backlog);
        requirePositive("netty.readTimeoutSeconds", readTimeoutSeconds);
        requirePositive("netty.writeTimeoutSeconds", writeTimeoutSeconds);
        requirePositive("netty.maxRequestBytes", maxRequestBytes);
        requirePositive("netty.maxJsonRequestBytes", maxJsonRequestBytes);
        requirePositive("netty.responseChunkBytes", responseChunkBytes);
        if (maxJsonRequestBytes > maxRequestBytes)
            throw new IllegalArgumentException("netty.maxJsonRequestBytes must not exceed netty.maxRequestBytes");
    }

    static ServerConfig fromSystemProperties() {
        return new ServerConfig(
                System.getProperty("host", "0.0.0.0"),
                Integer.getInteger("port", 8086),
                Integer.getInteger("netty.workerThreads", 0),
                Integer.getInteger("netty.backlog", 1024),
                Integer.getInteger("netty.readTimeoutSeconds", 300),
                Integer.getInteger("netty.writeTimeoutSeconds", 300),
                Integer.getInteger("netty.maxRequestBytes", DEFAULT_MAX_REQUEST_BYTES),
                Integer.getInteger("netty.maxJsonRequestBytes", DEFAULT_MAX_JSON_REQUEST_BYTES),
                Integer.getInteger("netty.responseChunkBytes", 64 * 1024),
                Boolean.parseBoolean(System.getProperty("netty.epoll", "true")));
    }

    private static void requirePositive(String name, int value) {
        if (value <= 0)
            throw new IllegalArgumentException(name + " must be greater than 0");
    }
}
