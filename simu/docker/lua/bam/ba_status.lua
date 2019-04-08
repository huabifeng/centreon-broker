local _ba_status = {}

local function build(ba_id)
  local now = os.time()
  local retval = {
    ba_id = ba_id,
    _type = 393217,
    in_downtime = 0,
    last_state_change = now,
    level_acknowledgement = 30,
    level_downtime = 40,
    level_nominal = 50,
    state = 1,
    state_changed = 0,
  }
  return retval
end

local ba_status = {
  name = "BA Status",
  build = function (stack, count)
    local ba_count = count.ba
    broker_log:info(0, "BUILD BA STATUS ; ba = " .. ba_count)
    for i = 1,ba_count do
      table.insert(stack, build(i))
    end
    broker_log:info(0, "BUILD BA STATUS => FINISHED")
  end,

  check = function (conn, count)
    local ba_count = count.ba
    broker_log:info(0, "CHECK BA STATUS")
    local retval = true
    local cursor, error_str = conn["cfg"]:execute("SELECT count(*) from mod_bam" )
    local row = cursor:fetch({}, "a")

    if tonumber(row['count(*)']) ~= ba_count then
      retval = false
    end
    if not retval then
      broker_log:info(0, "CHECK BA STATUS => NOT DONE")
    else
      broker_log:info(0, "CHECK BA STATUS => DONE")
    end
    return retval
  end
}

return ba_status
