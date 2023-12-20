create database staging;
use staging;
create table staging.StagingNews(id int auto_increment not null primary key,
						 title text,
                         image text,
                         category text,
						 desciption text,
                         content text,
                         author text,
                         tags text,
                         create_at text,
                         update_at text,
                         create_by text,
                         update_by text);
                         select * from staging.StagingNews;  
                         insert into staging.StagingNews(title, image, category, desciption, content, author, tags, create_at, update_at, create_by, update_by) 
                         VALUES ("a", "a.png", "Thể thao", "Hay", "Hay, kịch tính", "VTV", "Bóng đá", "2023-12-15 20:11:59", "2023-12-15 20:11:59", "admin", "admin");
                         insert into staging.StagingNews(title, image, category, desciption, content, author, tags, create_at, update_at, create_by, update_by) 
                         VALUES ("b", "b.png", "Thể thao", "Hay", "Vui, kịch tính", "VTV", "Bóng đá", "2023-12-15 20:11:59", "2023-12-15 20:11:59", "admin", "admin");
drop table staging.StagingNews;
drop database staging;
truncate table staging.stagingnews;

create table staging.news(id int auto_increment not null primary key,
						  title varchar(1000),
                          image text,
                          categoryId int references staging.category(id),
                          desciption text,
                          content text,
                          author varchar(100),
                          tags varchar(100),
                          create_at DateTime,
                          update_at DateTime,
                          create_by varchar(100),
                          update_by varchar(100));

create table staging.category(id int auto_increment not null primary key,
							  title varchar(300),
                              create_at DateTime,
                              update_at DateTime);
select *from staging.News;
select * from staging.Category;
drop table staging.News;
drop table staging.Category;