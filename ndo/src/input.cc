/*
** Copyright 2009-2011 Merethis
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

#include <algorithm>
#include <QScopedPointer>
#include <stdlib.h>
#include "events/events.hh"
#include "exceptions/basic.hh"
#include "mapping.hh"
#include "nagios/protoapi.h"
#include "ndo/input.hh"
#include "ndo/internal.hh"

using namespace com::centreon::broker;
using namespace com::centreon::broker::ndo;

/**************************************
*                                     *
*           Private Methods           *
*                                     *
**************************************/

/**
 *  Read a line of input.
 */
char const* input::_get_line() {
  size_t it;
  while ((it = _buffer.find_first_of('\n')) == std::string::npos) {
    QSharedPointer<io::data> data(_from->read());
    if (data.isNull() || !data->size())
      break ;
    _buffer.append(static_cast<char*>(data->memory()), data->size());
  }
  _old = _buffer.substr(0, it);
  _buffer.erase(0, it);
  return (_old.c_str());
}

/**
 *  Extract event parameters from the data stream.
 */
template <typename T>
T* input::_handle_event() {
  QScopedPointer<T> event(new T);
  int key;
  char const* key_str;
  char const* value_str;

  while (1) {
    key_str = _get_line();
    if (key_str) {
      typename std::map<int, getter_setter<T> >::const_iterator it;
      key = strtol(key_str, NULL, 10);
      if (NDO_API_ENDDATA == key)
        break ;
      value_str = strchr(key_str, '=');
      value_str = (value_str ? value_str + 1 : "");
      it = ndo_mapped_type<T>::map.find(key);
      if (it != ndo_mapped_type<T>::map.end())
        (*it->second.setter)(*event.data(), *it->second.member, value_str);
    }
    else {
      event.reset();
      break ;
    }
  }
  return (event.take());
}

/**************************************
*                                     *
*           Public Methods            *
*                                     *
**************************************/

/**
 *  Default constructor.
 */
input::input() {}

/**
 *  Copy constructor.
 *
 *  @param[in] i Object to copy.
 */
input::input(input const& i) : io::stream(i) {}

/**
 *  Destructor.
 */
input::~input() {}

/**
 *  Assignment operator.
 *
 *  @param[in] i Object to copy.
 *
 *  @return This object.
 */
input& input::operator=(input const& i) {
  io::stream::operator=(i);
  return (*this);
}

/**
 *  @brief Get the next available event.
 *
 *  Extract the next available event on the input stream, NULL if the
 *  stream is closed.
 *
 *  @return Next available event, NULL if stream is closed.
 */
QSharedPointer<io::data> input::read() {
  QScopedPointer<events::event> e;
  char const* line;

  // Get the next non-empty line.
  do {
    line = _get_line();
  } while (line && !line[0]);

  if (line) {
    int id = strtol(line, NULL, 10);
    switch (id) {
     case NDO_API_ACKNOWLEDGEMENTDATA:
      e.reset(_handle_event<events::acknowledgement>());
      break ;
     case NDO_API_COMMENTDATA:
      e.reset(_handle_event<events::comment>());
      break ;
     case NDO_API_RUNTIMEVARIABLES:
      e.reset(_handle_event<events::custom_variable>());
      break ;
     case NDO_API_CONFIGVARIABLES:
      e.reset(_handle_event<events::custom_variable_status>());
      break ;
     case NDO_API_DOWNTIMEDATA:
      e.reset(_handle_event<events::downtime>());
      break ;
     case NDO_API_EVENTHANDLERDATA:
      e.reset(_handle_event<events::event_handler>());
      break ;
     case NDO_API_FLAPPINGDATA:
      e.reset(_handle_event<events::flapping_status>());
      break ;
     case NDO_API_HOSTCHECKDATA:
      e.reset(_handle_event<events::host_check>());
      break ;
     case NDO_API_HOSTDEFINITION:
      e.reset(_handle_event<events::host>());
      break ;
     case NDO_API_HOSTDEPENDENCYDEFINITION:
      e.reset(_handle_event<events::host_dependency>());
      break ;
     case NDO_API_HOSTGROUPDEFINITION:
      e.reset(_handle_event<events::host_group>());
      break ;
     case NDO_API_HOSTGROUPMEMBERDEFINITION:
      e.reset(_handle_event<events::host_group_member>());
      break ;
     case NDO_API_HOSTPARENT:
      e.reset(_handle_event<events::host_parent>());
      break ;
     case NDO_API_STATECHANGEDATA:
      e.reset(_handle_event<events::host_state>());
      break ;
     case NDO_API_HOSTSTATUSDATA:
      e.reset(_handle_event<events::host_status>());
      break ;
     case NDO_API_PROCESSDATA:
      e.reset(_handle_event<events::instance>());
      break ;
     case NDO_API_PROGRAMSTATUSDATA:
      e.reset(_handle_event<events::instance_status>());
      break ;
     case NDO_API_LOGDATA:
      e.reset(_handle_event<events::log_entry>());
      break ;
     case NDO_API_COMMANDDEFINITION:
      e.reset(_handle_event<events::module>());
      break ;
     case NDO_API_NOTIFICATIONDATA:
      e.reset(_handle_event<events::notification>());
      break ;
     case NDO_API_SERVICECHECKDATA:
      e.reset(_handle_event<events::service_check>());
      break ;
     case NDO_API_SERVICEDEFINITION:
      e.reset(_handle_event<events::service>());
      break ;
     case NDO_API_SERVICEDEPENDENCYDEFINITION:
      e.reset(_handle_event<events::service_dependency>());
      break ;
     case NDO_API_SERVICEGROUPDEFINITION:
      e.reset(_handle_event<events::service_group>());
      break ;
     case NDO_API_SERVICEGROUPMEMBERDEFINITION:
      e.reset(_handle_event<events::service_group_member>());
      break ;
     case NDO_API_ADAPTIVESERVICEDATA:
      e.reset(_handle_event<events::service_state>());
      break ;
     case NDO_API_SERVICESTATUSDATA:
      e.reset(_handle_event<events::service_status>());
      break ;
     default:
      // Skip this event.
      while (id != NDO_API_ENDDATA) {
        line = _get_line();
        if (line)
          id = strtol(line, NULL, 10);
        else
          break ;
      }
      if (line)
        return (this->read());
    }
  }
  return (QSharedPointer<io::data>((io::data*)(e.take())));
}

/**
 *  Write data.
 *
 *  @param[in] d Object to copy.
 */
void input::write(QSharedPointer<io::data> d) {
  (void)d;
  throw (exceptions::basic() << "attempt to write from an NDO input object (software bug)");
  return ;
}
