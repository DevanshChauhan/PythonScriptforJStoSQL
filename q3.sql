select TargetingCriteria.targetingType as "Criteria Category" , count(matchedTargetingCriteria.impression) as "Ad Count" FROM matchedTargetingCriteria 
JOIN TargetingCriteria where matchedTargetingCriteria.criteria = TargetingCriteria.id GROUP by TargetingCriteria.targetingType
ORDER by "Ad Count" DESC LIMIT 10;