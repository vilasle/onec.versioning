CREATE TABLE public.versions (
    id integer NOT NULL,
    ref text NOT NULL,
    version_number integer NOT NULL,
    version_user text NOT NULL,
    content bytea NOT NULL,
    created_at timestamp without time zone
);