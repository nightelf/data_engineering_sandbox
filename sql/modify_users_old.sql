-- Run against the data_engineering database.
-- Exercises the CDC pipeline by mutating users_old: updates 5 rows, inserts 2
-- new users, and deletes 1 row. With the streaming job running, these changes
-- propagate to users_new.

-- Update 5 existing rows.
UPDATE users_old SET telephone = '555-9001'             WHERE username = 'obennett';
UPDATE users_old SET last_name = 'Carter-Reyes'          WHERE username = 'lcarter';
UPDATE users_old SET email = 'emma.dawson2@example.com'  WHERE username = 'edawson';
UPDATE users_old SET telephone = '555-9004'             WHERE username = 'nellis';
UPDATE users_old SET first_name = 'Ava-Marie'            WHERE username = 'afoster';

-- Insert 2 new users.
INSERT INTO users_old (first_name, last_name, email, username, password_hash, salt, telephone)
VALUES
    ('Priya',  'Anand',   'priya.anand@example.com',  'panand',  'c4ca4238a0b923820dcc509a6f75849b', 'aa11bb22', '555-0301'),
    ('Marcus', 'Webb',    'marcus.webb@example.com',   'mwebb',   'c81e728d9d4c2f636f067f89cc14862c', 'bb22cc33', '555-0302');

-- Delete 1 row.
DELETE FROM users_old WHERE username = 'hquinn';
