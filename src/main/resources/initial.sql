create table if not exists task (
    task_id serial primary key,
    title varchar(255),
    finished boolean,
    created_date timestamp
);
