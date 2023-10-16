SELECT strftime('%H', "impressionTime") AS `Hour`, COUNT(*) AS `Ad Count` FROM impressions GROUP BY `Hour` ORDER BY `Hour`;

