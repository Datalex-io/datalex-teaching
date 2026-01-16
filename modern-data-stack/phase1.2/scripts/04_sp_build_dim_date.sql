CREATE OR REPLACE PROCEDURE dw.sp_build_dim_date(p_start DATE, p_end DATE)
LANGUAGE plpgsql
AS $$
DECLARE
  d DATE;
BEGIN
  IF p_end < p_start THEN
    RAISE EXCEPTION 'p_end must be >= p_start';
  END IF;

  d := p_start;
  WHILE d <= p_end LOOP
    INSERT INTO dw.dim_date (
      date_id, date, year, quarter, month, day,
      iso_year, iso_week, day_of_week, day_name, is_weekend
    )
    VALUES (
      (to_char(d, 'YYYYMMDD'))::INT,
      d,
      EXTRACT(YEAR FROM d)::INT,
      EXTRACT(QUARTER FROM d)::INT,
      EXTRACT(MONTH FROM d)::INT,
      EXTRACT(DAY FROM d)::INT,
      EXTRACT(ISOYEAR FROM d)::INT,
      EXTRACT(WEEK FROM d)::INT,
      EXTRACT(ISODOW FROM d)::INT,
      to_char(d, 'FMDay'),
      (EXTRACT(ISODOW FROM d)::INT IN (6,7))
    )
    ON CONFLICT (date_id) DO NOTHING;

    d := d + INTERVAL '1 day';
  END LOOP;
END;
$$;
