-- Run against the data_engineering database.
-- Drops and recreates the legacy `users_old` and modern `users_new` tables for
-- a clean start, seeds 20 users into users_old, and seeds 10 native users
-- directly into users_new (these pre-date the migration).

-- bcrypt support (crypt() / gen_salt('bf')).
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS users_old;
DROP TABLE IF EXISTS users_new;

-- Legacy users table: separate username + salt fields.
CREATE TABLE users_old (
    id            SERIAL PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL,
    username      TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    salt          TEXT NOT NULL,
    telephone     TEXT
);

-- Modern users table: email is the username, no separate salt (the new
-- hashing algorithm stores the salt internally), and an enabled flag that
-- now defaults to TRUE. password_hash is nullable: passwords are NOT migrated
-- from users_old (migrated rows stay NULL until the user resets).
CREATE TABLE users_new (
    id            SERIAL PRIMARY KEY,
    users_old_id  INTEGER UNIQUE,            -- links back to users_old.id (CDC key)
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL,
    password_hash TEXT,
    telephone     TEXT,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    migration_dt  TIMESTAMP
);

-- Seed 20 users into users_old.
INSERT INTO users_old (first_name, last_name, email, username, password_hash, salt, telephone)
VALUES
    ('Olivia',   'Bennett',   'olivia.bennett@example.com',   'obennett',   '8f14e45fceea167a5a36dedd4bea2543', 'a1b2c3d4', '555-0101'),
    ('Liam',     'Carter',    'liam.carter@example.com',      'lcarter',    'e2fc714c4727ee9395f324cd2e7f331f', 'b2c3d4e5', '555-0102'),
    ('Emma',     'Dawson',    'emma.dawson@example.com',       'edawson',    '7c6a180b36896a0a8c02787eeafb0e4c', 'c3d4e5f6', '555-0103'),
    ('Noah',     'Ellis',     'noah.ellis@example.com',        'nellis',     '6cb75f652a9b52798eb6cf2201057c73', 'd4e5f6a7', '555-0104'),
    ('Ava',      'Foster',    'ava.foster@example.com',        'afoster',    '1d0258c2440a8d19e716292b231e3190', 'e5f6a7b8', '555-0105'),
    ('Ethan',    'Grant',     'ethan.grant@example.com',       'egrant',     '5f4dcc3b5aa765d61d8327deb882cf99', 'f6a7b8c9', '555-0106'),
    ('Sophia',   'Hayes',     'sophia.hayes@example.com',      'shayes',     '6f1ed002ab5595859014ebf0951522d9', 'a7b8c9d0', '555-0107'),
    ('Mason',    'Irwin',     'mason.irwin@example.com',       'mirwin',     '0d107d09f5bbe40cade3de5c71e9e9b7', 'b8c9d0e1', '555-0108'),
    ('Isabella', 'Jensen',    'isabella.jensen@example.com',   'ijensen',    'd8578edf8458ce06fbc5bb76a58c5ca4', 'c9d0e1f2', '555-0109'),
    ('Lucas',    'Klein',     'lucas.klein@example.com',       'lklein',     'b109f3bbbc244eb82441917ed06d618b', 'd0e1f2a3', '555-0110'),
    ('Mia',      'Lawson',    'mia.lawson@example.com',        'mlawson',    '96e79218965eb72c92a549dd5a330112', 'e1f2a3b4', '555-0111'),
    ('James',    'Morgan',    'james.morgan@example.com',      'jmorgan',    '25d55ad283aa400af464c76d713c07ad', 'f2a3b4c5', '555-0112'),
    ('Charlotte','Nash',      'charlotte.nash@example.com',    'cnash',      'e99a18c428cb38d5f260853678922e03', 'a3b4c5d6', '555-0113'),
    ('Benjamin', 'Owens',     'benjamin.owens@example.com',    'bowens',     'fcea920f7412b5da7be0cf42b8c93759', 'b4c5d6e7', '555-0114'),
    ('Amelia',   'Patel',     'amelia.patel@example.com',      'apatel',     '7110eda4d09e062aa5e4a390b0a572ac', 'c5d6e7f8', '555-0115'),
    ('Henry',    'Quinn',     'henry.quinn@example.com',       'hquinn',     '1a1dc91c907325c69271ddf0c944bc72', 'd6e7f8a9', '555-0116'),
    ('Harper',   'Reed',      'harper.reed@example.com',       'hreed',      'a87ff679a2f3e71d9181a67b7542122c', 'e7f8a9b0', '555-0117'),
    ('Alexander','Sims',      'alexander.sims@example.com',    'asims',      'e4da3b7fbbce2345d7772b0674a318d5', 'f8a9b0c1', '555-0118'),
    ('Evelyn',   'Turner',    'evelyn.turner@example.com',     'eturner',    '1679091c5a880faf6fb5e6087eb1b2dc', 'a9b0c1d2', '555-0119'),
    ('Daniel',   'Vance',     'daniel.vance@example.com',      'dvance',     '8f14e45fceea167a5a36dedd4bea2543', 'b0c1d2e3', '555-0120');

-- Seed 10 native users directly into users_new (they pre-date the migration,
-- so migration_dt stays NULL). Each password_hash is a bcrypt of a random
-- 12-character string; enabled uses the new TRUE default.
INSERT INTO users_new (first_name, last_name, email, password_hash, telephone)
VALUES
    ('Aria',     'Whitfield', 'aria.whitfield@example.com',   crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0201'),
    ('Caleb',    'Mercer',    'caleb.mercer@example.com',     crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0202'),
    ('Nora',     'Sullivan',  'nora.sullivan@example.com',    crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0203'),
    ('Felix',    'Barros',    'felix.barros@example.com',     crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0204'),
    ('Ruby',     'Castellano','ruby.castellano@example.com',  crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0205'),
    ('Theo',     'Nakamura',  'theo.nakamura@example.com',    crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0206'),
    ('Iris',     'Delacroix', 'iris.delacroix@example.com',   crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0207'),
    ('Oscar',    'Pennington','oscar.pennington@example.com', crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0208'),
    ('Lena',     'Vasquez',   'lena.vasquez@example.com',     crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0209'),
    ('Milo',     'Ashford',   'milo.ashford@example.com',     crypt(substr(md5(random()::text), 1, 12), gen_salt('bf')), '555-0210');
