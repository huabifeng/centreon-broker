--package.path = package.path .. ";/home/admin/?.lua"
local mysql = require "luasql.mysql"

local blue = string.char(27) .. "[34m"
local red = string.char(27) .. "[31m"
local green = string.char(27) .. "[32m"
local reset = string.char(27) .. "[0m"

local simu = {
  log_file = "/tmp/simu.log",
  host_count = 1,
  poller_count = 1,
  conn = nil,
  stack = {},
  step_build = 1,
  step_check = 1,
}

local step = {
  require('neb.instances'),
  require('neb.hosts'),
  require('neb.hostgroups'),
  require('neb.hostgroup_members'),
  require('neb.custom_variables'),
  require('neb.custom_variable_status'),
  require('neb.comments'),
  require('neb.services'),
  require('neb.servicegroups'),
  require('neb.service_checks'),
  require('neb.service_status'),
  require('neb.downtimes'),
  require('neb.host_checks'),
  require('neb.host_status'),
  require('neb.acknowledgements'),
  require('neb.event_handler'),
  require('bam.ba_status'),
  require('bam.dimension_truncate_table_signal'),
}

-- Instances                  => 18
step[1].count = {
  instance = 2,
  continue = true,
}

-- Hosts per instance         => 312
step[2].count = {
  host = 10,
  instance = step[1].count.instance,
  continue = true,
}

-- Hostgroups
step[3].count = {
  group = 10,
  continue = true,
}

-- Hostgroups members
step[4].count = {
  host = step[2].count.host,
  instance = step[2].count.instance,
  hostgroup = 1,
  continue = true,
}

-- Custom variables per host  =>
step[5].count = {
  cv = 30,
  host = step[2].count.host,
  instance = step[1].count.instance,
  continue = true,
}

-- Custom variables status per host  =>
step[6].count = {
  cv = 30,
  host = step[2].count.host,
  instance = step[1].count.instance,
  continue = true,
}

-- Comments per host
step[7].count = {
  comment = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  continue = true,
}

-- Services per host          => 20
step[8].count = {
  service = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  continue = true,
}

-- Servicegroups
step[9].count = {
  servicegroup = 20,
  continue = true,
}

-- Service checks
step[10].count = {
  service = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  continue = true,
}

-- Services per host          => 20
step[11].count = {
  service = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  metric = 10,
  continue = true,
}

-- Downtimes per host
step[12].count = {
  host = 5,
  continue = true,
}

-- Host checks and logs per instance
step[13].count = {
  host = step[2].count.host,
  instance = step[1].count.instance,
  continue = true,
}

-- Host status
step[14].count = {
  host = step[2].count.host,
  instance = step[1].count.instance,
  continue = true,
}

-- Acknowledgements
step[15].count = {
  service = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  continue = true,
}

-- Event handlers
step[16].count = {
  service = 50,
  host = step[2].count.host,
  instance = step[2].count.instance,
  continue = true,
}

-- Ba status
step[17].count = {
  ba = 100,
  update_started = true,
  continue = true,
}

-- Table truncate signal
step[18].count = {
  update_started = true,
  continue = false,
}

function os.capture(cmd, raw)
  local f = assert(io.popen(cmd, 'r'))
  local s = assert(f:read('*a'))
  f:close()
  if raw then return s end
  s = string.gsub(s, '^%s+', '')
  s = string.gsub(s, '%s+$', '')
  s = string.gsub(s, '[\n\r]+', ' ')
  return s
end

function init(conf)
  math.randomseed(os.time())
  os.remove("/tmp/simu.log")
  broker_log:set_parameters(3, simu.log_file)
  local env = mysql.mysql()
  simu.conn = {}
  simu.conn["storage"] = env:connect('centreon_storage', conf['login'], conf['password'], conf['db_addr'], 3306)
  if not simu.conn["storage"] then
    broker_log:error(0, "No connection to database")
    error("No connection to database")
  end

  simu.conn["cfg"] = env:connect('centreon', conf['login'], conf['password'], conf['db_addr'], 3306)
  if not simu.conn["cfg"] then
    broker_log:error(0, "No connection to cfg database")
    error("No connection to cfg database")
  end

  -- Some clean up
  local cursor, error_str = simu.conn["storage"]:execute("DELETE FROM data_bin;")
  cursor, error_str = simu.conn["storage"]:execute("DELETE FROM metrics;")
  cursor, error_str = simu.conn["cfg"]:execute("DELETE FROM mod_bam;")
end

function read()
  if (simu.step_build == 1 or (simu.step_build > 1 and step[simu.step_build - 1].count.continue)) and #simu.stack == 0 then
    if simu.step_build == 1 then
      print(red .. "===== START =====" .. reset)
    end

    -- Building step in db
    if step[simu.step_build] then
      broker_log:info(0, "Build Step " .. simu.step_build)
      print(green .. "BUILD " .. reset .. step[simu.step_build].name)
      step[simu.step_build].build(simu.stack, step[simu.step_build].count)
      print("   stack size " .. #simu.stack)
      simu.step_build = simu.step_build + 1
    end
  end

  -- Check of step in db
  if simu.step_check < simu.step_build or not step[simu.step_check].count.continue then
    if step[simu.step_check].check(simu.conn, step[simu.step_check].count) then
      print(blue .. "CHECK " .. reset .. step[simu.step_check].name .. " DONE")
      if not step[simu.step_check].count.continue then
        broker_log:info(0, "No more step")
        local output = os.capture("ps ax | grep \"\\<cbd\\>\" | grep -v grep | awk '{print $1}' ", 1)
        if output ~= "" then
          broker_log:info(0, "SEND COMMAND: kill " .. output)
          os.execute("kill -9 " .. output)
        end
      end
      simu.step_check = simu.step_check + 1
      broker_log:info(0, "Check Step " .. simu.step_check)
    end
  end

  -- Need to pop elemnts from the stack
  if #simu.stack > 0 then
    if #simu.stack % 100 == 0 then
      broker_log:info(0, "Stack contains " .. #simu.stack .. " elements")
    end
    broker_log:info(2, "EVENT SEND: " .. broker.json_encode(simu.stack[1]))
    return table.remove(simu.stack, 1)
  end
  return nil
end
