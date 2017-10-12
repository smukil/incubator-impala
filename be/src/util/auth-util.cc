// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "service/client-request-state.h"
#include "util/auth-util.h"
#include "util/network-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace std;
using boost::algorithm::is_any_of;

DECLARE_string(principal);
DECLARE_string(be_principal);

namespace impala {

// Pattern for hostname substitution.
static const string HOSTNAME_PATTERN = "_HOST";

  const string& GetEffectiveUser(const TSessionState& session) {
    if (session.__isset.delegated_user && !session.delegated_user.empty()) {
      return session.delegated_user;
    }
    return session.connected_user;
  }

  const std::string& GetEffectiveUser(const ImpalaServer::SessionState& session) {
    return session.do_as_user.empty() ? session.connected_user : session.do_as_user;
  }

  Status CheckProfileAccess(const string& user, const string& effective_user,
      bool has_access) {
    if (user.empty() || (user == effective_user && has_access)) return Status::OK();
    stringstream ss;
    ss << "User " << user << " is not authorized to access the runtime profile or "
       << "execution summary.";
    return Status(ss.str());
  }

// Replaces _HOST with the hostname if it occurs in the principal string.
Status ReplacePrincipalHostFormat(std::string* out_principal) {
  // Replace the string _HOST in principal with our hostname.
  size_t off = out_principal->find(HOSTNAME_PATTERN);
  if (off != string::npos) {
    string hostname;
    RETURN_IF_ERROR(GetHostname(&hostname));
    out_principal->replace(off, HOSTNAME_PATTERN.size(), hostname);
  }
  return Status::OK();
}

Status GetExternalKerberosPrincipal(std::string* out_principal) {
  *out_principal = std::string();
  if (FLAGS_principal.empty()) return Status::OK();
  *out_principal = FLAGS_principal;
  RETURN_IF_ERROR(ReplacePrincipalHostFormat(out_principal));
  return Status::OK();
}

Status GetInternalKerberosPrincipal(std::string* out_principal) {
  *out_principal = std::string();
  if (FLAGS_principal.empty()) return Status::OK();
  if (FLAGS_be_principal.empty()) {
    *out_principal = FLAGS_principal;
    RETURN_IF_ERROR(ReplacePrincipalHostFormat(out_principal));
  } else {
    *out_principal = FLAGS_be_principal;
    RETURN_IF_ERROR(ReplacePrincipalHostFormat(out_principal));
  }
  return Status::OK();
}

Status DissectKerberosPrincipal(const std::string& principal, std::string* service_name,
    std::string* hostname, std::string* realm) {
  vector<string> names;
  split(names, principal, is_any_of("/@"));

  if (names.size() != 3) {
    return Status(strings::Substitute("Kerberos principal should be of the form: "
        "<service>/<hostname>@<realm> - got: $0", principal));
  }

  *service_name = names[0];
  *hostname = names[1];
  *realm = names[2];

  return Status::OK();
}

}
