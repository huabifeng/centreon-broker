/*
** Copyright 2015 Merethis
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

#include <sstream>
#include "com/centreon/broker/database_query.hh"
#include "com/centreon/broker/dumper/db_loader.hh"
#include "com/centreon/broker/dumper/entries/ba.hh"
#include "com/centreon/broker/dumper/entries/ba_type.hh"
#include "com/centreon/broker/dumper/entries/kpi.hh"
#include "com/centreon/broker/dumper/entries/organization.hh"
#include "com/centreon/broker/dumper/entries/state.hh"
#include "com/centreon/broker/exceptions/msg.hh"

using namespace com::centreon::broker;
using namespace com::centreon::broker::dumper;

/**************************************
*                                     *
*           Public Methods            *
*                                     *
**************************************/

/**
 *  Default constructor.
 *
 *  @param[in] db_cfg  Database configuration.
 */
db_loader::db_loader(database_config const& db_cfg)
  : _db_cfg(db_cfg), _poller_id(0), _state(NULL) {}

/**
 *  Destructor.
 */
db_loader::~db_loader() {}

/**
 *  Load configuration from database.
 *
 *  @param[out] state      Configuration state.
 *  @param[in]  poller_id  ID of poller for which we want to load
 *                         configuration.
 */
void db_loader::load(entries::state& state, unsigned int poller_id) {
  // Initialization.
  _db.reset(new database(_db_cfg));
  _state = &state;
  _poller_id = poller_id;

  // Database loading.
  _load_organizations();
  _load_ba_types();
  _load_bas();
  _load_kpis();

  // Cleanup.
  _db.reset();
  _state = NULL;
  _poller_id = 0;

  return ;
}

/**************************************
*                                     *
*           Private Methods           *
*                                     *
**************************************/

/**
 *  Load BA types.
 */
void db_loader::_load_ba_types() {
  database_query q(*_db);
  q.run_query(
      "SELECT ba_type_id, name, slug, description"
      "  FROM cfg_bam_ba_types",
      "db_reader: could not load BA types from DB");
  while (q.next()) {
    entries::ba_type b;
    b.ba_type_id = q.value(0).toUInt();
    b.name = q.value(1).toString();
    b.slug = q.value(2).toString();
    b.description = q.value(3).toString();
    _state->get_ba_types().push_back(b);
  }
  return ;
}

/**
 *  Load BAs.
 */
void db_loader::_load_bas() {
  std::ostringstream query;
  query << "SELECT b.ba_id, b.name, b.description, b.level_w,"
           "       b.level_c, b.organization_id, b.ba_type_id"
           "  FROM cfg_bam AS b"
           "  INNER JOIN cfg_bam_poller_relations AS pr"
           "    ON b.ba_id=pr.ba_id"
           "  WHERE b.activate='1'"
           "    AND pr.poller_id=" << _poller_id;
  database_query q(*_db);
  q.run_query(
      query.str(),
      "db_reader: could not load configuration of BAs from DB");
  while (q.next()) {
    entries::ba b;
    b.enable = true;
    b.poller_id = _poller_id;
    b.ba_id = q.value(0).toUInt();
    b.name = q.value(1).toString();
    b.description = q.value(2).toString();
    b.level_warning = q.value(3).toDouble();
    b.level_critical = q.value(4).toDouble();
    b.organization_id = q.value(5).toUInt();
    b.type_id = q.value(6).toUInt();
    _state->get_bas().push_back(b);
  }
  return ;
}

/**
 *  Load KPIs.
 */
void db_loader::_load_kpis() {
  std::ostringstream query;
  query << "SELECT k.kpi_id, k.kpi_type, k.host_id, k.service_id,"
           "       k.id_indicator_ba, k.id_ba, k.meta_id, k.boolean_id,"
           "       COALESCE(k.drop_warning, iw.impact),"
           "       COALESCE(k.drop_critical, ic.impact),"
           "       COALESCE(k.drop_unknown, iu.impact)"
           "  FROM cfg_bam_kpi AS k"
           "  INNER JOIN cfg_bam_poller_relations AS pr"
           "    ON k.id_ba=pr.ba_id"
           "  LEFT JOIN cfg_bam_impacts AS iw"
           "    ON k.drop_warning_impact_id=iw.id_impact"
           "  LEFT JOIN cfg_bam_impacts AS ic"
           "    ON k.drop_critical_impact_id=ic.id_impact"
           "  LEFT JOIN cfg_bam_impacts AS iu"
           "    ON k.drop_unknown_impact_id=iu.id_impact"
           "  WHERE k.activate='1'"
           "    AND pr.poller_id=" << _poller_id;
  database_query q(*_db);
  q.run_query(
      query.str(),
      "db_reader: could not load configuration of KPIs from DB");
  while (q.next()) {
    entries::kpi k;
    k.enable = true;
    k.poller_id = _poller_id;
    k.kpi_id = q.value(0).toUInt();
    k.kpi_type = q.value(1).toInt() + 1;
    k.host_id = q.value(2).toUInt();
    k.service_id = q.value(3).toUInt();
    k.ba_indicator_id = q.value(4).toUInt();
    k.ba_id = q.value(5).toUInt();
    k.meta_id = q.value(6).toUInt();
    k.boolean_id = q.value(7).toUInt();
    k.drop_warning = q.value(8).toDouble();
    k.drop_critical = q.value(9).toDouble();
    k.drop_unknown = q.value(10).toDouble();
    _state->get_kpis().push_back(k);
  }
  return ;
}

/**
 *  Load organizations.
 */
void db_loader::_load_organizations() {
  std::ostringstream query;
  query << "SELECT o.organization_id, o.name, o.shortname"
           "  FROM cfg_pollers AS p"
           "  INNER JOIN cfg_organizations AS o"
           "    ON p.organization_id=o.organization_id"
           "  WHERE p.poller_id=" << _poller_id;
  database_query q(*_db);
  q.run_query(
      query.str(),
      "db_reader: could not load organization from DB");
  if (!q.next())
    throw (exceptions::msg() << "db_reader: poller " << _poller_id
           << " has no organization: cannot load remaining tables");
  entries::organization o;
  o.enable = true;
  o.organization_id = q.value(0).toUInt();
  o.name = q.value(1).toString();
  o.shortname = q.value(2).toString();
  _state->get_organizations().push_back(o);
  return ;
}