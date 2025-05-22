DO $$
DECLARE
    curr_date DATE := '2020-05-14';
    end_date DATE := CURRENT_DATE;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO snp.times (time_id)
        VALUES (curr_date)
        ON CONFLICT (time_id) DO NOTHING;

        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$;
