/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "storage/StorageFlags.h"

using nebula::Status;

static Status setupLogging() {
    if (!FLAGS_redirect_stdout) {
        return Status::OK();
    }

    auto dup = [](const std::string &filename, FILE *stream) -> Status {
        auto path = FLAGS_log_dir + "/" + filename;
        auto fd = ::open(path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1) {
            return Status::Error(
                "Failed to create or open `%s': %s", path.c_str(), ::strerror(errno));
        }
        if (::dup2(fd, ::fileno(stream)) == -1) {
            return Status::Error(
                "Failed to ::dup2 from `%s' to stdout: %s", path.c_str(), ::strerror(errno));
        }
        ::close(fd);
        return Status::OK();
    };

    NG_RETURN_IF_ERROR(dup(FLAGS_stdout_log_file, stdout));
    NG_RETURN_IF_ERROR(dup(FLAGS_stderr_log_file, stderr));
    return Status::OK();
}
