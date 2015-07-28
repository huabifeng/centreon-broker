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

#ifndef CCB_EXTCMD_STREAM_HH
#  define CCB_EXTCMD_STREAM_HH

#  include <string>
#  include "com/centreon/broker/timestamp.hh"
#  include "com/centreon/broker/io/stream.hh"
#  include "com/centreon/broker/namespace.hh"
#  include "com/centreon/broker/file/fifo.hh"

CCB_BEGIN()

namespace           extcmd {
  /**
   *  @class stream stream.hh "com/centreon/broker/extcmd/stream.hh"
   *  @brief Command file stream.
   *
   *  The class converts commands to NEB events.
   */
  class             stream : public io::stream {
  public:
                    stream(std::string const& filename);
                    ~stream();
    bool            read(
                      misc::shared_ptr<io::data>& d,
                      time_t deadline = (time_t)-1);
    void            statistics(io::properties& tree) const;
    unsigned int    write(misc::shared_ptr<io::data> const& d);

  private:
                    stream(stream const& other);
    stream&         operator=(stream const& other);

    std::string     _filename;
    file::fifo      _fifo;
  };
}

CCB_END()

#endif // !CCB_EXTCMD_STREAM_HH