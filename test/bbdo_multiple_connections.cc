/*
** Copyright 2012-2013 Merethis
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

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <QSqlError>
#include <QSqlQuery>
#include <QVariant>
#include <sstream>
#include "com/centreon/broker/exceptions/msg.hh"
#include "test/cbd.hh"
#include "test/config.hh"
#include "test/engine.hh"
#include "test/generate.hh"
#include "test/misc.hh"
#include "test/vars.hh"

using namespace com::centreon::broker;

#define DB_NAME "broker_bbdo_multiple_connections"
#define ENGINE_COUNT 3

/**
 *  Check that multiple connections to cbd work properly.
 *
 *  @return EXIT_SUCCESS on success.
 */
int main() {
  // Error flag.
  bool error(true);

  // Variables that need cleaning.
  std::list<host> hosts[ENGINE_COUNT];
  std::list<service> services[ENGINE_COUNT];
  std::string engine_config_path[ENGINE_COUNT];
  for (unsigned int i(0); i < ENGINE_COUNT; ++i)
    engine_config_path[i] = tmpnam(NULL);
  engine daemon[ENGINE_COUNT];
  cbd broker;

  try {
    // Prepare database.
    QSqlDatabase db(config_db_open(DB_NAME));

    // Prepare monitoring engine configuration parameters.
    for (unsigned int i(0); i < 3; ++i) {
      generate_hosts(hosts[i], 1);
      generate_services(services[i], hosts[i], i + 1);
      std::string cbmod_loading;
      {
        std::ostringstream oss;
        oss << "broker_module=" << CBMOD_PATH << " "
            << PROJECT_SOURCE_DIR
            << "/test/cfg/bbdo_multiple_connections_"
            << (!i ? 0 : 1) << ".xml";
        cbmod_loading = oss.str();
      }

      // Generate monitoring engine configuration files.
      config_write(
        engine_config_path[i].c_str(),
        cbmod_loading.c_str(),
        hosts + i,
        services + i);
    }

    // Start Broker daemon.
    broker.set_config_file(
      PROJECT_SOURCE_DIR "/test/cfg/bbdo_multiple_connections_2.xml");
    broker.start();
    sleep_for(2 * MONITORING_ENGINE_INTERVAL_LENGTH);
    broker.update();

    // Start engines.
    for (unsigned int i(0); i < ENGINE_COUNT; ++i) {
      std::string engine_config_file(engine_config_path[i]);
      engine_config_file.append("/nagios.cfg");
      daemon[i].set_config_file(engine_config_file);
      daemon[i].start();
      sleep_for(2 * MONITORING_ENGINE_INTERVAL_LENGTH);
    }
    sleep_for(15 * MONITORING_ENGINE_INTERVAL_LENGTH);

    // Terminate monitoring engine.
    for (int i(ENGINE_COUNT - 1); i >= 0; --i)
      daemon[i].stop();

    // Terminate Broker daemon.
    broker.stop();

    // Check host count.
    {
      std::ostringstream query;
      query << "SELECT COUNT(host_id) FROM hosts";
      QSqlQuery q(db);
      if (!q.exec(query.str().c_str()))
        throw (exceptions::msg() << "cannot read host count from DB: "
               << q.lastError().text().toStdString().c_str());
      if (!q.next()
          || (q.value(0).toUInt() != 2)
          || q.next())
        throw (exceptions::msg() << "invalid host count: got "
               << q.value(0).toUInt() << ", expected 2");
    }

    // Check service count.
    {
      std::ostringstream query;
      query << "SELECT COUNT(service_id) FROM services";
      QSqlQuery q(db);
      if (!q.exec(query.str().c_str()))
        throw (exceptions::msg()
               << "cannot read service count from DB: "
               << q.lastError().text().toStdString().c_str());
      if (!q.next()
          || (q.value(0).toUInt() != 5)
          || q.next())
        throw (exceptions::msg() << "invalid service count: got "
               << q.value(0).toUInt() << ", expected 5");
    }

    // Success.
    error = false;
  }
  catch (std::exception const& e) {
    std::cerr << e.what() << std::endl;
  }
  catch (...) {
    std::cerr << "unknown exception" << std::endl;
  }

  // Cleanup.
  for (int i(ENGINE_COUNT - 1); i >= 0; --i)
  daemon[i].stop();
  broker.stop();
  for (unsigned int i(0); i < ENGINE_COUNT; ++i)
    config_remove(engine_config_path[i].c_str());
  config_db_close(DB_NAME);
  for (unsigned int i(0); i < ENGINE_COUNT; ++i) {
    free_hosts(hosts[i]);
    free_services(services[i]);
  }

  return (error ? EXIT_FAILURE : EXIT_SUCCESS);
}