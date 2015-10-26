/*
** Copyright 2015 Centreon
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

#ifndef CCB_WATCHDOG_BROKER_INSTANCE_CONFIGURATION_HH
#  define CCB_WATCHDOG_BROKER_INSTANCE_CONFIGURATION_HH

#  include <string>
#  include "com/centreon/broker/namespace.hh"

CCB_BEGIN()

namespace         watchdog {
  /**
   *  @class broker_instance_configuration broker_instance_configuration.hh "com/centreon/broker/watchdog/broker_instance_configuration.hh"
   *  @brief Configuration of a centreon broker instance.
   */
  class           broker_instance_configuration {
  public:
                  broker_instance_configuration();
                  broker_instance_configuration(std::string const& name,
                    std::string const& config_file,
                    bool should_run,
                    bool should_reload,
                    unsigned int seconds_per_tentative);
                  ~broker_instance_configuration();
                  broker_instance_configuration(
                    broker_instance_configuration const& other);
    broker_instance_configuration&
                  operator=(broker_instance_configuration const& other);

    std::string const&
                  get_name() const throw();
    std::string const&
                  get_config_file() const throw();
    bool          should_run() const throw();
    bool          should_reload() const throw();
    unsigned int  seconds_per_tentative() const throw();

  private:
    std::string   _name;
    std::string   _config_file;
    bool          _run;
    bool          _reload;
    unsigned int  _seconds_per_tentative;
  };
}

CCB_END()

#endif // !CCB_WATCHDOG_BROKER_INSTANCE_CONFIGURATION_HH
