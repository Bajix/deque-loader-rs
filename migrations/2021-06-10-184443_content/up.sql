CREATE TABLE content (
  id serial PRIMARY KEY,
  title varchar(256) NOT NULL
);

CREATE TABLE users_content (
  user_id serial REFERENCES users(id),
  content_id serial REFERENCES content(id),
  PRIMARY KEY (user_id, content_id)
);