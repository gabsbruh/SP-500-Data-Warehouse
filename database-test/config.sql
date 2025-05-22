GRANT CONNECT ON DATABASE snp500 TO readonly_user;
GRANT USAGE ON SCHEMA snp TO readonly_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO readonly_user;

SET ROLE readonly_user;
SELECT * 
FROM snp.stocks s
     INNER JOIN snp.companies c ON s.comp_ticker = c.comp_ticker
     INNER JOIN snp.times t ON s.time_id = t.time_id
     INNER JOIN snp.currencies c2 ON t.time_id = c2.time_id;
RESET ROLE;