/*
 * Copyright 2016 Capital One Services, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __TERADATA_LOAD_H
#define __TERADATA_LOAD_H

#include <connection.h>
#include <schema.h>
#include <DMLGroup.h>


using namespace teradata::client::API;

class TeradataUpdate {
      char* select_stmt;
      Schema* table_schema;
      DMLGroup* dml_group;
      Connection* conn;
      char* error_msg;
      TD_ErrorType error_type;
  private:
      bool table_set;
      bool connected;
      void handle_error(int return_value);

  public:
      int connection_status;
      int row_status;

      TeradataUpdate(std::string server, std::string username, std::string password);
      ~TeradataUpdate();

      void set_table(std::string table_name);
      void add_column(char* name, int data_type, int size, int precision, int scale);
      void add_schema();
      void add_dmlgroup(char* dml, int dml_option);
      void add_attribute(int attr_name, char* attr_value);
      void add_attribute(int attr_name, int attr_value);
      int initiate();
      int put_row(char* row, int length);
      int put_buffer(char* buffer, int length);
      void end_acquisition();
      void apply_rows();
      int checkpoint();
      int get_event(int event_type, char** event_data, int* data_len, int event_index);
      void get_error_info();
      char* error_message();
      void close();
};

#endif
