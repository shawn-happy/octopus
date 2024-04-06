create table if not exists t_job
(
    id               varchar(50) primary key,
    name             varchar(100) not null,
    description      varchar(500),
    step_ids         varchar(500) not null,
    version          varchar(50)  not null,
    create_time      bigint       not null,
    update_time      bigint       not null
);

create table if not exists t_step
(
    id              varchar(50) primary key,
    name            varchar(100) not null,
    job_id          varchar(50)  not null,
    type            varchar(50)  not null,
    identify        varchar(50)  not null,
    description     varchar(255),
    input           varchar(500) null,
    output          varchar(50)  null,
    step_attributes varchar(500) not null,
    create_time     bigint       not null,
    update_time     bigint       not null
);

create table if not exists t_step_attribute
(
    id          varchar(50) primary key,
    job_id      varchar(50) not null,
    step_id     varchar(50) not null,
    code        varchar(50) not null,
    value       text        not null,
    create_time bigint      not null,
    update_time bigint      not null
);