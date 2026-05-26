-- Apply once on the production DB:
--   psql $DB_URL -f backend/migrations/2026_05_26_notification_events.sql

CREATE TABLE IF NOT EXISTS public.notification_events (
    id integer NOT NULL,
    user_id integer NOT NULL,
    type character varying(64) NOT NULL,
    source character varying(255) NOT NULL DEFAULT ''::character varying,
    external_id character varying(255) NOT NULL DEFAULT ''::character varying,
    title character varying(512) NOT NULL DEFAULT ''::character varying,
    thumbnail_url character varying(1024) DEFAULT ''::character varying,
    product_url character varying(2048) DEFAULT ''::character varying,
    old_price bigint,
    new_price bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_at timestamp with time zone
);

CREATE SEQUENCE IF NOT EXISTS public.notification_events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.notification_events_id_seq OWNED BY public.notification_events.id;

ALTER TABLE ONLY public.notification_events
    ALTER COLUMN id SET DEFAULT nextval('public.notification_events_id_seq'::regclass);

ALTER TABLE ONLY public.notification_events
    ADD CONSTRAINT notification_events_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.notification_events
    ADD CONSTRAINT notification_events_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id);

CREATE INDEX IF NOT EXISTS ix_notification_events_user_id ON public.notification_events USING btree (user_id);
CREATE INDEX IF NOT EXISTS ix_notification_events_created_at ON public.notification_events USING btree (created_at);
