create table nodes(
    id integer,
    cores integer,
    memslices integer,
    primary key (id));

create table applications(
    id integer,
    primary key (id));

create table pending(
    id integer not null auto_increment,
    application integer,
    cores integer,
    memslices integer,
    status varchar(36),
	current_node integer,
	controllable__node integer,
	foreign key (controllable__node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (id));

create table placed(
    application integer,
    node integer,
    cores integer,
    memslices integer,
	foreign key (node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (application, node));
