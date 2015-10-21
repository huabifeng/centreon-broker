--
-- Monitored hosts.
--
CREATE TABLE rt_hosts (
  host_id int NOT NULL,
  name varchar(255) NOT NULL,
  instance_id int NOT NULL,

  acknowledged boolean default NULL,
  acknowledgement_type smallint default NULL,
  active_checks boolean default NULL,
  address varchar(75) default NULL,
  alias varchar(100) default NULL,
  check_attempt smallint default NULL,
  check_command text default NULL,
  check_freshness boolean default NULL,
  check_interval double precision default NULL,
  check_period varchar(75) default NULL,
  check_type smallint default NULL,
  checked boolean default NULL,
  command_line text default NULL,
  default_active_checks boolean default NULL,
  default_event_handler_enabled boolean default NULL,
  default_flap_detection boolean default NULL,
  default_notify boolean default NULL,
  enabled boolean NOT NULL default true,
  event_handler varchar(255) default NULL,
  event_handler_enabled boolean default NULL,
  execution_time double precision default NULL,
  first_notification_delay double precision default NULL,
  flap_detection boolean default NULL,
  flap_detection_on_down boolean default NULL,
  flap_detection_on_unreachable boolean default NULL,
  flap_detection_on_up boolean default NULL,
  flapping boolean default NULL,
  freshness_threshold double precision default NULL,
  high_flap_threshold double precision default NULL,
  last_check int default NULL,
  last_hard_state smallint default NULL,
  last_hard_state_change int default NULL,
  last_notification int default NULL,
  last_state_change int default NULL,
  last_time_down int default NULL,
  last_time_unreachable int default NULL,
  last_time_up int default NULL,
  last_update int default NULL,
  latency double precision default NULL,
  low_flap_threshold double precision default NULL,
  max_check_attempts smallint default NULL,
  next_check int default NULL,
  next_host_notification int default NULL,
  no_more_notifications boolean default NULL,
  notification_interval double precision default NULL,
  notification_number smallint default NULL,
  notification_period varchar(75) default NULL,
  notify boolean default NULL,
  notify_on_down boolean default NULL,
  notify_on_downtime boolean default NULL,
  notify_on_flapping boolean default NULL,
  notify_on_recovery boolean default NULL,
  notify_on_unreachable boolean default NULL,
  obsess_over_host boolean default NULL,
  output text default NULL,
  percent_state_change double precision default NULL,
  perfdata text default NULL,
  real_state smallint default NULL,
  retry_interval double precision default NULL,
  scheduled_downtime_depth smallint NOT NULL default 0,
  should_be_scheduled boolean default NULL,
  stalk_on_down boolean default NULL,
  stalk_on_unreachable boolean default NULL,
  stalk_on_up boolean default NULL,
  state smallint default NULL,
  state_type smallint default NULL,

  UNIQUE (host_id),
  INDEX (instance_id),
  INDEX (name),
  FOREIGN KEY (instance_id) REFERENCES rt_instances (instance_id)
    ON DELETE CASCADE
);
