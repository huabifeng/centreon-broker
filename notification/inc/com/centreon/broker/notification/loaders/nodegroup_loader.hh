/*
** Copyright 2011-2013 Merethis
**
** This file is part of Centreon Broker.
**
** Centreon Broker is free software: you can redistribute it and/or
** modify it under the terms of the GNU General Public License version 2
** as published by the Free Software Foundation.
**
** Centreon Broker is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
** General Public License for more details.
**
** You should have received a copy of the GNU General Public License
** along with Centreon Broker. If not, see
** <http://www.gnu.org/licenses/>.
*/

#ifndef CCB_NOTIFICATION_LOADERS_NODEGROUP_LOADER_HH
#  define CCB_NOTIFICATION_LOADERS_NODEGROUP_LOADER_HH

#  include <string>
#  include <QSqlDatabase>
#  include <QSqlQuery>
#  include "com/centreon/broker/namespace.hh"
#  include "com/centreon/broker/notification/builders/nodegroup_builder.hh"

CCB_BEGIN()

namespace       notification {
  /**
   *  @class nodegroup_loader nodegroup_loader.hh "com/centreon/broker/notification/loaders/nodegroup_loader.hh"
   *  @brief Loader for node_group objects.
   *
   *  This loader loads the nodegroups from the database.
   */
  class         nodegroup_loader {
  public:
                nodegroup_loader();

    void        load(QSqlDatabase* db, nodegroup_builder* output);
  };

}

CCB_END()

#endif // !CCB_NOTIFICATION_LOADERS_NODEGROUP_LOADER_HH