/*
** Copyright 2018 Centreon
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
**     http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
**
** For more information : contact@centreon.com
*/
#ifndef CCB_STORAGE_MYSQL_HH
#  define CCB_STORAGE_MYSQL_HH

#include <vector>
#include "com/centreon/broker/misc/shared_ptr.hh"
#include "mysql_thread.hh"
#include "com/centreon/broker/database.hh"

CCB_BEGIN()

namespace           storage {
  /**
   *  @class mysql mysql.hh "com/centreon/broker/storage/mysql.hh"
   *  @brief Class managing the mysql connection
   *
   *  Here is a binding to the C MySQL connector.
   */
  class                 mysql {
   public:
                        mysql(database_config const& db_cfg);
                        ~mysql();
    void                prepare_query(std::string const& query);
    void                run_query(std::string const& query, int thread = -1);
    void                run_query_with_callback(
                          std::string const& query,
                          mysql_callback fn,
                          int thread = -1);
    bool                finish();

   private:
    database_config     _db_cfg;

    std::vector<mysql_thread*>
                        _vector;
    database::version   _version;
    std::vector<misc::shared_ptr<mysql_thread> >
                        _thread;
    int                 _current_thread;
  };
}

CCB_END()

#  endif  //CCB_STORAGE_MYSQL_HH
