/*
** Copyright 2017 Centreon
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

#include <QDir>
#include <cstdlib>
#include <memory>
#include <gtest/gtest.h>
#include "com/centreon/broker/config/applier/init.hh"
#include "com/centreon/broker/file/splitter.hh"

using namespace com::centreon::broker;
using namespace com::centreon::broker::file;

#define BIG 50000
#define RETENTION_DIR "/tmp/"
#define RETENTION_FILE "test-read-write-conflict-queue"

class ReadWriteConflictPersistentFile : public ::testing::Test {

 public:
  void SetUp() {
    //config::applier::init();
    _path = RETENTION_DIR RETENTION_FILE;
  }

  void TearDown() {
    _remove_files();
    //config::applier::deinit();
  }

 protected:
  std::string                    _path;

  void _remove_files() {
    system("cat /tmp/test-read-write-conflict-queue");
    QString dirname(_path.c_str());
    int idx(dirname.lastIndexOf("/"));
    QString path = dirname.mid(idx + 1) + "*";
    dirname.resize(idx + 1);
    QDir dir(dirname);
    QStringList filters_list;
    filters_list << path;
    QStringList entries(dir.entryList(filters_list));
    for (QStringList::iterator it(entries.begin()), end(entries.end());
      it != end; ++it) {
      QFile::remove(dirname + *it);
    }
  }
};

/**
 * Given an already existing retention file.
 * When I open the file to read it,
 * Then data are available from the beginning
 */
TEST_F(ReadWriteConflictPersistentFile, FirstRead) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));

  char txt[20];
  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_01_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  splitter->close();
  size_t length(strlen(txt));
  {
    unsigned int count_test1(0);
    txt[length] = 0;
    try {
      while (true) {
        splitter->read(txt, length);
        if (strncmp(txt, "Test_01", 7) == 0) {
          count_test1++;
          std::cout << "count_test1 = " << count_test1 << std::endl;
        }
        else {
          ASSERT_FALSE("Bad content!");
        }
      }
    }
    catch (std::exception const& e) {
      std::cout << "No more to read: " << e.what() << std::endl;
    }
    splitter->close();
    ASSERT_EQ(count_test1, 10);
  }
}

/**
 * Given an already existing retention file.
 * When I open the file to write into it,
 * Then data are appended to the file
 */
TEST_F(ReadWriteConflictPersistentFile, FirstWrite) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));

  char txt[20];
  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_01_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  size_t length(strlen(txt));
  splitter->close();

  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_02_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  splitter->close();

  {
    unsigned int count_test1(0);
    unsigned int count_test2(0);
    txt[length] = 0;

    try {
      while (true) {
        splitter->read(txt, length);
        if (strncmp(txt, "Test_01", 7) == 0) {
          count_test1++;
          std::cout << "count_test1 = " << count_test1 << std::endl;
        }
        else if (strncmp(txt, "Test_02", 7) == 0) {
          count_test2++;
          std::cout << "count_test2 = " << count_test2 << std::endl;
        }
        else {
          ASSERT_FALSE("Bad content!");
        }
      }
    }
    catch (std::exception const& e) {
      std::cout << "No more to read: " << e.what() << std::endl;
    }
    splitter->close();
    ASSERT_EQ(count_test1, 10);
    ASSERT_EQ(count_test2, 10);
  }
}

/**
 * Given an already existing retention file.
 * When I open the file to read it and also write
 * Then read data are fully accessible and written data are appended to the file
 */
TEST_F(ReadWriteConflictPersistentFile, FirstReadThenWrite) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));

  char txt[20];
  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_01_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  size_t length(strlen(txt));
  splitter->close();

  {
    // Just to have the final '\0'
    txt[length] = 0;

    try {
      for (int i = 0; i < 10; ++i) {
        splitter->read(txt, length);
        if (strncmp(txt, "Test_01", 7))
          ASSERT_FALSE("Bad content!");

        sprintf(txt, "Test_02_%03d\n", i);
        splitter->write(txt, strlen(txt));
      }
    }
    catch (std::exception const& e) {
      std::cout << "No more to read: " << e.what() << std::endl;
    }
    splitter->close();
  }
}

/**
 * Given an already existing retention file.
 * When I open the file to write into it and also read it
 * Then read data are fully accessible and written data are appended to the file
 */
TEST_F(ReadWriteConflictPersistentFile, FirstWriteThenRead) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));

  char txt[20];
  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_01_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  size_t length(strlen(txt));
  splitter->close();

  {
    txt[length] = 0;

    try {
      for (int i = 0; i < 10; ++i) {
        sprintf(txt, "Test_02_%03d\n", i);
        splitter->write(txt, strlen(txt));

        splitter->read(txt, length);
        if (strncmp(txt, "Test_01", 7))
          ASSERT_FALSE("Bad content!");
      }
    }
    catch (std::exception const& e) {
      std::cout << "No more to read: " << e.what() << std::endl;
    }
    splitter->close();
  }
}

/**
 * Given a not existing retention file.
 * When I open the file to read it and also write
 * Then read data are fully accessible and written data are appended to the file
 */
TEST_F(ReadWriteConflictPersistentFile, FirstEmptyReadThenWrite) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));
  char txt[20];
  int count(0);

  for (int i = 0; i < 10; ++i) {
    try {
      splitter->read(txt, 12);
      if (strncmp(txt, "Test_02", 7))
        ASSERT_FALSE("Bad content!");
      count++;
    }
    catch (std::exception const& e) {}

    sprintf(txt, "Test_02_%03d\n", i);
    splitter->write(txt, strlen(txt));
  }
  splitter->close();
  ASSERT_EQ(count, 9);
}

/**
 * Given a not existing retention file.
 * When I open the file to write into it and also read it
 * Then read data are fully accessible and written data are appended to the file
 */
TEST_F(ReadWriteConflictPersistentFile, FirstEmptyWriteThenRead) {
  splitter_factory f;
  std::auto_ptr<splitter> splitter(f.new_cfile_splitter(
                _path,
                fs_file::open_read_write_truncate,  // ignored
                500,
                false));
  char txt[20];
  int count(0);

  for (int i = 0; i < 10; ++i) {
    sprintf(txt, "Test_03_%03d\n", i);
    splitter->write(txt, strlen(txt));
    splitter->read(txt, 12);
    if (strncmp(txt, "Test_03", 7))
      ASSERT_FALSE("Bad content!");
    count++;
  }
  splitter->close();
  ASSERT_EQ(count, 10);
}
