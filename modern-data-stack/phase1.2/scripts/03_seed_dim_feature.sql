CREATE OR REPLACE PROCEDURE dw.sp_seed_dim_feature()
LANGUAGE plpgsql
AS $$
BEGIN
  TRUNCATE TABLE dw.dim_feature CASCADE;

  INSERT INTO dw.dim_feature(feature_id, name, category, introduced_at)
  VALUES
    -- Editor
    ('f001', 'Rich Text Editor', 'Editor', '2024-12-14 00:00:00'),
    ('f004', 'Schema Editor', 'Editor', '2024-12-10 00:00:00'),
    ('f005', 'Metadata Editor', 'Editor', '2024-12-12 00:00:00'),
    ('f006', 'Documentation Editor', 'Editor', '2024-12-19 00:00:00'),
    ('f018', 'Visual Editor', 'Editor', '2024-10-16 00:00:00'),

    -- AI
    ('f002', 'AI Assistant', 'AI', '2024-12-21 00:00:00'),
    ('f008', 'AI Search', 'AI', '2024-11-24 00:00:00'),
    ('f010', 'AI Recommendations', 'AI', '2024-11-28 00:00:00'),
    ('f014', 'AI Data Classification', 'AI', '2024-11-08 00:00:00'),
    ('f015', 'AI Auto Tagging', 'AI', '2024-12-03 00:00:00'),

    -- Catalog
    ('f003', 'Data Catalog', 'Catalog', '2024-11-17 00:00:00'),
    ('f007', 'Business Glossary', 'Catalog', '2024-12-03 00:00:00'),
    ('f012', 'Dataset Inventory', 'Catalog', '2024-10-24 00:00:00'),
    ('f013', 'Data Asset Discovery', 'Catalog', '2024-10-11 00:00:00'),

    -- Governance
    ('f009', 'Access Policies', 'Governance', '2024-11-29 00:00:00'),
    ('f011', 'Audit Logs', 'Governance', '2024-11-08 00:00:00'),
    ('f016', 'Data Ownership', 'Governance', '2024-11-27 00:00:00'),
    ('f017', 'Compliance Reports', 'Governance', '2024-12-17 00:00:00')
  ON CONFLICT (feature_id) DO NOTHING;
END;
$$;
