/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef KVSTORE_LISTENER_FACTORY_H_
#define KVSTORE_LISTENER_FACTORY_H_

#include "kvstore/Listener.h"
#include "kvstore/plugins/elasticsearch/ESListener.h"
#include "kvstore/plugins/kafka/KafkaListener.h"

namespace nebula {
namespace kvstore {

class ListenerFactory {
public:
    template <typename... Args>
    static std::shared_ptr<Listener> createListener(meta::cpp2::ListenerType type, Args&&... args) {
        if (type == meta::cpp2::ListenerType::ELASTICSEARCH) {
            return std::make_shared<ESListener>(std::forward<Args>(args)...);
        } else if (type == meta::cpp2::ListenerType::KAFKA) {
            LOG(INFO) << "Create Kafka Listener";
            return std::make_shared<KafkaListener>(std::forward<Args>(args)...);
        } else {
            LOG(FATAL) << "Should not reach here";
        }
        return nullptr;
    }
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_LISTENER_FACTORY_H_
