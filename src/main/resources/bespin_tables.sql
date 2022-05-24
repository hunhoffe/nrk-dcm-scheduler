create table nodes(
    id integer,
    cores integer,
    memslices integer,
    primary key (id));

create table applications(
    id integer,
    primary key (id));

create table allocation_state(
    application integer,
    node integer,
    cores integer,
    memslices integer,
	foreign key (node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (application, node));

create table pending_allocations(
    id integer,
    application integer,
    cores integer,
    memslices integer,
    status varchar(36),
	current_node integer,
	controllable__node integer,
	foreign key (controllable__node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (id));
