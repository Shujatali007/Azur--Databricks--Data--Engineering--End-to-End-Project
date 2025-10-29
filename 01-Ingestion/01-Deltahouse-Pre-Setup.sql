-- Switch to the pricing_analytics catalog
use catalog pricing_analytics;

-- Create the processrunlogs schema if it does not exist
create schema if not exists processrunlogs;

-- Create the deltalakehouse_process_runs table if it does not exist
create table if not exists processrunlogs.deltalakehouse_process_runs (
  process_name string,
  processed_file_table_date date,
  process_status string
);

-- Add the Processed_table_DateTime column to the table
alter table pricing_analytics.processrunlogs.deltalakehouse_process_runs
add columns ( Processed_table_DateTime timestamp);

-- Reference the deltalakehouse_process_runs table
pricing_analytics.processrunlogs.deltalakehouse_process_runs

-- Reference the deltalakehouse_process_runs table
pricing_analytics.processrunlogs.deltalakehouse_process_runs