WITH RankedAdvertisers AS (

WITH RankingAdvertiser AS(
    SELECT 
        adv.advertiserName,
        COUNT(*) AS advcount
    From
        advertiserInfo as adv
    JOIN 
        impressions as imp on imp.advertiserInfo = adv.advertiserName
    GROUP BY 
        adv.advertiserName
    ORDER BY
        advcount DESC
    LIMIT 10
)

    SELECT
        ra."advertiserName",
        tc."targetingType",
        tc."targetingValue",
        COUNT(*) AS combination_count,
        ROW_NUMBER() OVER (PARTITION BY ra."advertiserName" ORDER BY COUNT(*) DESC) AS rn
    FROM
        RankingAdvertiser ra
    JOIN
        impressions im ON ra."advertiserName" = im."advertiserInfo"
    JOIN
        matchedTargetingCriteria mtc ON im."id" = mtc."impression"
    JOIN
        "TargetingCriteria" tc ON mtc."criteria" = tc."id"
    GROUP BY
        ra."advertiserName",
        tc."targetingType",
        tc."targetingValue" 
)
SELECT
    "advertiserName" AS Advertiser,
    "targetingType" AS "Criteria Type",
    "targetingValue" AS Criterion
FROM
    RankedAdvertisers
WHERE
    rn <= 10
ORDER BY
    Advertiser ASC,
    rn DESC ;