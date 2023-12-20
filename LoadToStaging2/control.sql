create database control;
use control;
create table control.data_files(id SERIAL,
								df_config_id bigint,
                                name varchar(1000),
                                row_count bigint,
                                status varchar(255),
                                data_range_from date,
                                data_range_to date,
                                note text,
                                created_at date,
                                updated_at date,
                                created_by varchar(255),
                                updated_by varchar(255));
CREATE TABLE control.data_file_configs (
  `id` int(11) NOT NULL,
  `name` varchar(1000) NOT NULL,
  `description` text NOT NULL,
  `source_path` varchar(1000) DEFAULT NULL,
  `location` varchar(1000) DEFAULT NULL,
  `format` varchar(255) NOT NULL,
  `columns` text NOT NULL,
  `create_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `update_at` datetime DEFAULT NULL,
  `create_by` varchar(1000) NOT NULL DEFAULT 'root',
  `update_by` varchar(1000) DEFAULT NULL
); 
select * from control.data_file_configs;

INSERT INTO control.data_file_configs (`id`, `name`, `description`, `source_path`, `location`, `format`, `columns`, `create_at`, 
`update_at`, `create_by`, `update_by`) VALUES
(1, 'aggregate_news_articles', 'This is config table news_articles', 'null', 'D:\\HK7\\Warehouse', 'present_news_articles', 
'id,title,description,content,image,category,author_name,date,tags', '2023-12-14 08:50:55', NULL, 'root', NULL);

SELECT location FROM control.data_file_configs WHERE create_at = '2023-12-14 08:50:55' ORDER BY create_at DESC LIMIT 1;

select * from control.data_file_configs;
select * from control.data_files;
insert into control.data_files(df_config_id, name, row_count, status, data_range_from, data_range_to, note, created_at, updated_at, created_by, updated_by)
 values(1, "Load to Staging", 0, "ready", "2023-10-30 0:00:00", "2023-10-30: 23:59:00", "task ready", "2023-11-29 21:11:00", "2023-11-29 21:11:00", "userA", "userA");
insert into control.data_files (df_config_id, name, row_count, status, data_range_from, data_range_to, note, created_at, updated_at, created_by, updated_by)
values (1, "Extract from source to file", 0, "successful", "2023-10-30", "2023-10-30", "task ready", current_date(), current_date(), "admin", "admin");
Select * from control.data_files where name = "Load to Staging" and created_at = "2023-11-29 21:11:00" and status = "ready";

select * from control.data_files;
drop table control.data_files;
SELECT location FROM control.data_file_configs ORDER BY create_at DESC LIMIT 1;

create table control.Logs (id int not null auto_increment primary key,
						   event varchar(100),
                           status varchar(255),
                           note varchar(1000),
						   create_at date);
                           
                           select * from control.Logs;
                          drop table control.Logs;		