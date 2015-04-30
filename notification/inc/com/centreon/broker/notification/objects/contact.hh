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

#ifndef CCB_NOTIFICATION_CONTACT_HH
#  define CCB_NOTIFICATION_CONTACT_HH

#  include <map>
#  include <vector>
#  include <string>
#  include "com/centreon/broker/namespace.hh"
#  include "com/centreon/broker/notification/objects/defines.hh"
#  include "com/centreon/broker/notification/utilities/ptr_typedef.hh"

CCB_BEGIN()

namespace   notification {
  namespace objects {
    /**
     *  @class contact contact.hh "com/centreon/broker/notification/objects/contact.hh"
     *  @brief Contact object.
     *
     *  The object containing a contact.
     */
    class                      contact {
    public:
                               DECLARE_SHARED_PTR(contact);

                               contact();
                               contact(contact const& obj);
                               contact& operator=(contact const& obj);

      unsigned int             get_id() const throw();
      void                     set_id(unsigned int);
      std::string const&       get_description() const throw();
      void                     set_description(std::string const& desc);

    private:
      unsigned int             _id;
      std::string              _description;
    };
  }
}

CCB_END()

#endif // !CCB_NOTIFICATION_CONTACT_HH