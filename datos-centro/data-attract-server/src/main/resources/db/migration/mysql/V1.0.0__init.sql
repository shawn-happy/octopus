create table if not exists dc_datasource
(
    id          bigint auto_increment primary key,
    code        varchar(100) not null,
    name        varchar(100) not null,
    type        varchar(100) not null,
    description text,
    config      text         not null,
    creator     varchar(100) not null,
    updater     varchar(100) not null,
    create_time bigint       not null,
    update_time bigint       not null
)