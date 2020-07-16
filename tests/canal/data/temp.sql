
CREATE TABLE t
(
    id          INT AUTO_INCREMENT,
    t_date      DATE,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

insert into t(t_date) values("1582-10-1");
insert into t(t_date) values("1582-10-2");
insert into t(t_date) values("1582-10-3");
insert into t(t_date) values("1582-10-4");
insert into t(t_date) values("1582-10-5");
insert into t(t_date) values("1582-10-6");
insert into t(t_date) values("1582-10-7");
insert into t(t_date) values("1582-10-8");
insert into t(t_date) values("1582-10-9");
insert into t(t_date) values("1582-10-10");
insert into t(t_date) values("1582-10-11");
insert into t(t_date) values("1582-10-12");
insert into t(t_date) values("1582-10-13");
insert into t(t_date) values("1582-10-14");
insert into t(t_date) values("1582-10-15");
insert into t(t_date) values("1582-10-16");
insert into t(t_date) values("1582-10-17");
insert into t(t_date) values("1582-10-18");
insert into t(t_date) values("1582-10-19");
insert into t(t_date) values("1582-10-20");
insert into t(t_date) values("1582-10-21");
insert into t(t_date) values("1582-10-22");
insert into t(t_date) values("1582-10-23");
insert into t(t_date) values("1582-10-24");
insert into t(t_date) values("1582-10-25");
insert into t(t_date) values("1582-10-26");
insert into t(t_date) values("1582-10-27");
insert into t(t_date) values("1582-10-28");
insert into t(t_date) values("1582-10-29");
insert into t(t_date) values("1582-10-30");


CREATE TABLE text_
(
    id          INT AUTO_INCREMENT,
    t_text      TEXT,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

  CREATE TABLE blob_
(
    id          INT AUTO_INCREMENT,
    t_blob      BLOB,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;

    CREATE TABLE year_
(
    id          INT AUTO_INCREMENT,
    t_year      YEAR,
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8
  COLLATE = utf8_bin;