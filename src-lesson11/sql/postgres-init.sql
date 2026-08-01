-- Lesson 11 source of truth: a small OLTP orders table captured from the WAL.
-- `discount_code` is intentionally absent. scripts/migrate_discount_code.py
-- adds it only after the downstream has proven it can accept both schemas.
--
-- This is deliberately the same `orders` table students built in Lesson 1 and
-- carried through Lessons 4 and 5 -- same columns, same types, same NUMERIC
-- precision, plus Lesson 5's `updated_at`. The CHECK constraints and the
-- touch trigger are the only additions. Nothing about the capstone's source
-- of truth should look new.

CREATE TABLE IF NOT EXISTS public.orders (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INTEGER        NOT NULL CHECK (customer_id > 0),
    amount      NUMERIC(10, 2) NOT NULL CHECK (amount >= 0),
    status      TEXT           NOT NULL DEFAULT 'pending'
                               CHECK (status IN ('pending', 'paid', 'shipped',
                                                 'delivered', 'cancelled')),
    created_at  TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS orders_customer_id_idx ON public.orders (customer_id);
CREATE INDEX IF NOT EXISTS orders_updated_at_idx ON public.orders (updated_at);

CREATE OR REPLACE FUNCTION public.touch_order_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS orders_touch_updated_at ON public.orders;
CREATE TRIGGER orders_touch_updated_at
BEFORE UPDATE ON public.orders
FOR EACH ROW EXECUTE FUNCTION public.touch_order_updated_at();

-- The key is enough for correct deletes; FULL makes before/after inspection in
-- the lesson and the verifier more useful.
ALTER TABLE public.orders REPLICA IDENTITY FULL;
