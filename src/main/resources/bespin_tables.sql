create table nodes(
    id integer,
    cores integer,
    memslices integer
);

create table applications(
    id integer    
);

create table cores(
    id integer,
    application integer,
	current_node integer,
	controllable__node integer,
	foreign key (controllable__node) references nodes(id),
	foreign key (application) references applications(id),
	primary key (application, id)
);

create table memslices(
    id integer,
    application integer,
    current_node integer,
    controllable__node integer,
    foreign key (controllable__node) references nodes(id),
    foreign key (application) references applications(id),
    primary key (application, id)
);

