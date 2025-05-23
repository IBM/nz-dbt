create table {schema}.expected_dep_macro (
	foo VARCHAR(50),
	bar VARCHAR(50)
);

create table {schema}.expected_local_macro (
	foo2 VARCHAR(50),
	bar2 VARCHAR(50)
);

create table {schema}.seed (
	id integer,
	updated_at timestamp
);

insert into {schema}.expected_dep_macro (foo, bar)
values ('arg1', 'arg2');

insert into {schema}.expected_local_macro (foo2, bar2)
values ('arg1', 'arg2'), ('arg3', 'arg4');

insert into {schema}.seed (id, updated_at)
values (1, '2017-01-01'), (2, '2017-01-02');
