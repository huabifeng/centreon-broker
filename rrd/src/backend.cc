/*
** Copyright 2011-2012 Centreon
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

#include "com/centreon/broker/rrd/backend.hh"

using namespace com::centreon::broker::rrd;

/**************************************
*                                     *
*            Public Methods           *
*                                     *
**************************************/

/**
 *  Default constructor.
 */
backend::backend() {}

/**
 *  Copy constructor.
 *
 *  @param[in] b Object to copy.
 */
backend::backend(backend const& b) {
  (void)b;
}

/**
 *  Destructor.
 */
backend::~backend() {}

/**
 *  Assignment operator.
 *
 *  @param[in] b Object to copy.
 *
 *  @return This object.
 */
backend& backend::operator=(backend const& b) {
  (void)b;
  return (*this);
}
