create table nodes(
    id integer,
    cores integer,
    primary key (id));

create table applications(
    id integer,
    primary key (id));

create table allocations(
    id integer,
    application integer,
    cores integer,
    status varchar(36),
	current_node integer,
	controllable__node integer,
	foreign key (controllable__node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (id));
