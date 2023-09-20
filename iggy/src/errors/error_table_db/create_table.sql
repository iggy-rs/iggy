CREATE TABLE error_codes
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT             NOT NULL UNIQUE,
    code          INTEGER UNSIGNED NOT NULL UNIQUE,
    template      TEXT,
    signature     TEXT,
    converts_from TEXT,
    documentation TEXT
);
