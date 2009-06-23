/*
**  Copyright 2009 MERETHIS
**  This file is part of CentreonBroker.
**
**  CentreonBroker is free software: you can redistribute it and/or modify it
**  under the terms of the GNU General Public License as published by the Free
**  Software Foundation, either version 2 of the License, or (at your option)
**  any later version.
**
**  CentreonBroker is distributed in the hope that it will be useful, but
**  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
**  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
**  for more details.
**
**  You should have received a copy of the GNU General Public License along
**  with CentreonBroker.  If not, see <http://www.gnu.org/licenses/>.
**
**  For more information : contact@centreon.com
*/

#ifndef SERVICE_STATUS_H_
# define SERVICE_STATUS_H_

# include <string>
# include <sys/types.h>
# include "host_service_status.h"

namespace               CentreonBroker
{
  class                 EventSubscriber;

  /**
   *  The ServiceStatusEvent represents the corresponding event generated by
   *  Nagios. It has all fields specified in the NDO database schema.
   */
  class                 ServiceStatus : public HostServiceStatus
  {
   private:
    enum                String
    {
      SERVICE_DESCRIPTION = 0,
      STRING_NB
    };
    enum                TimeT
    {
      LAST_TIME_CRITICAL = 0,
      LAST_TIME_OK,
      LAST_TIME_UNKNOWN,
      LAST_TIME_WARNING,
      TIMET_NB
    };
    std::string        strings_[STRING_NB];
    time_t             timets_[TIMET_NB];
    void               InternalCopy(const ServiceStatus& sse);

   public:
                       ServiceStatus();
                       ServiceStatus(const ServiceStatus& sse);
                       ~ServiceStatus();
    ServiceStatus&     operator=(const ServiceStatus& sse);
    int                GetType() const throw ();
    // Getters
    time_t             GetLastTimeCritical() const throw ();
    time_t             GetLastTimeOk() const throw ();
    time_t             GetLastTimeUnknown() const throw ();
    time_t             GetLastTimeWarning() const throw ();
    const std::string& GetServiceDescription() const throw ();
    // Setters
    void               SetLastTimeCritical(time_t ltc) throw ();
    void               SetLastTimeOk(time_t lto) throw ();
    void               SetLastTimeUnknown(time_t ltu) throw ();
    void               SetLastTimeWarning(time_t ltw) throw ();
    void               SetServiceDescription(const std::string& sd);
  };
}

#endif /* !SERVICE_STATUS_H_ */
