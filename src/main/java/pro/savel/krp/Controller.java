// Copyright 2019-2020 Sergey Savelev
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pro.savel.krp;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pro.savel.krp.objects.Message;
import pro.savel.krp.objects.Record;
import pro.savel.krp.objects.TopicInfo;

import java.util.Collection;

@RestController
@RequestMapping("/")
public class Controller {

    private final Service service;

    public Controller(Service service) {
        this.service = service;
    }

    @GetMapping(path = "/")
    public String getVersion() {
        return "1.10.7";
    }

    @GetMapping(path = "/{topic}")
    public TopicInfo getTopicInfo(@PathVariable String topic) {
        return service.getTopicInfo(topic);
    }

    @GetMapping(path = "/{topic}/{partition}")
    public Collection<Record> getData(@PathVariable String topic,
                                      @PathVariable int partition,
                                      @RequestParam long offset,
                                      @RequestParam(required = false) Long timeout,
                                      @RequestParam(required = false) Long limit,
                                      @RequestParam(required = false) String idHeader,
                                      @RequestHeader(required = false) String groupId,
                                      @RequestHeader(required = false) String clientId) {
        return service.getData(topic, partition, offset, timeout, limit, idHeader, groupId, clientId);
    }

    @PostMapping(path = "/{topic}")
    @ResponseStatus(HttpStatus.CREATED)
    public void postData(@PathVariable String topic, @RequestBody Message message) {
        service.postData(topic, message);
    }
}
